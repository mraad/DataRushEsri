package com.esri;

import com.pervasive.datarush.operators.CompositeOperator;
import com.pervasive.datarush.operators.CompositionContext;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.model.SimpleModelPort;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.ScalarInputField;
import com.pervasive.datarush.ports.physical.ScalarOutputField;
import com.pervasive.datarush.ports.physical.StringInputField;
import com.pervasive.datarush.ports.physical.StringOutputField;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.types.Field;
import com.pervasive.datarush.types.RecordTokenTypeBuilder;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.HashMap;

/**
 */
public class LUTOperator extends CompositeOperator implements RecordPipelineOperator
{
    private final RecordPort m_input = newRecordInput("input");
    private final RecordPort m_lookup = newRecordInput("lookup");
    private final RecordPort m_output = newRecordOutput("output");

    private class Builder extends ExecutableOperator
    {
        final RecordPort input = newRecordInput("lookup");
        final SimpleModelPort<HashMap> model = newOutput("model",
                new SimpleModelPort.Factory<HashMap>(HashMap.class));

        @JsonCreator
        private Builder()
        {
        }

        @Override
        protected void computeMetadata(final StreamingMetadataContext context)
        {
            context.parallelize(ParallelismStrategy.NON_PARALLELIZABLE);
        }

        @Override
        protected void execute(final ExecutionContext context)
        {
            final HashMap hashMap = new HashMap();
            final RecordInput recordInput = input.getInput(context);
            final StringInputField keyField = (StringInputField) recordInput.getField("KEY");
            final StringInputField valField = (StringInputField) recordInput.getField("VAL");
            while (recordInput.stepNext())
            {
                hashMap.put(keyField.asString(), valField.asString());
            }
            model.setModel(context, hashMap);
        }
    }

    private class Walker extends ExecutableOperator
    {
        final RecordPort input = newRecordInput("input");
        final SimpleModelPort<HashMap> model = newInput("model",
                new SimpleModelPort.Factory<HashMap>(HashMap.class));
        final RecordPort output = newRecordOutput("output");

        @JsonCreator
        private Walker()
        {
        }

        @Override
        protected void computeMetadata(final StreamingMetadataContext context)
        {
            context.parallelize(ParallelismStrategy.NEGOTIATE_BASED_ON_SOURCE);

            final RecordTokenTypeBuilder builder = new RecordTokenTypeBuilder();
            for (Field field : input.getType(context))
            {
                builder.addField(field);
            }
            // builder.addField(METERS, TokenTypeConstant.DOUBLE);

            output.setType(context, builder.toType());

            output.setOutputDataOrdering(context,
                    input.getSourceDataOrdering(context));

            output.setOutputDataDistribution(context,
                    input.getSourceDataDistribution(context));

        }

        @Override
        protected void execute(final ExecutionContext context)
        {
            final HashMap hashMap = model.getModel(context);

            final RecordInput recordInput = input.getInput(context);
            final RecordOutput recordOutput = output.getOutput(context);

            final ScalarInputField[] scalarInputFields = new ScalarInputField[input.getType(context).size()];
            final ScalarOutputField[] scalarOutputFields = new ScalarOutputField[scalarInputFields.length];

            for (int i = 0; i < scalarInputFields.length; i++)
            {
                scalarInputFields[i] = recordInput.getField(i);
                scalarOutputFields[i] = recordOutput.getField(i);
            }

            final StringInputField keyIn = (StringInputField) recordInput.getField("KEY");
            final StringOutputField keyOut = (StringOutputField) recordOutput.getField("KEY");

            while (recordInput.stepNext())
            {
                for (int i = 0; i < scalarInputFields.length; i++)
                {
                    scalarOutputFields[i].set(scalarInputFields[i]);
                }
                final String val = (String) hashMap.get(keyIn.asString());
                keyOut.set(val == null ? "N/A" : val);
                recordOutput.push();
            }
            recordOutput.pushEndOfData();
        }
    }

    @JsonCreator
    public LUTOperator()
    {
    }

    @Override
    public RecordPort getInput()
    {
        return m_input;
    }

    @Override
    public RecordPort getOutput()
    {
        return m_output;
    }

    public RecordPort getLookup()
    {
        return m_lookup;
    }

    @Override
    protected void compose(final CompositionContext context)
    {
        final Builder builder = context.add(new Builder());
        context.connect(m_lookup, builder.input);

        final Walker walker = context.add(new Walker());
        context.connect(builder.model, walker.model);
        context.connect(m_input, walker.input);
        context.connect(walker.output, m_output);
    }

}
