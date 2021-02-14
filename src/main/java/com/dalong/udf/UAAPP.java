package com.dalong.udf;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.OutputDerivation;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import javax.inject.Inject;
import java.util.List;

public class UAAPP {
    @FunctionTemplate(names = {"parse_user_agent"}, isDeterministic = false, derivation = UAGenOutput.class)
    public static class UA implements SimpleFunction {
        @Param
        VarCharHolder input;
        @Output
        ComplexWriter outWriter;
        @Inject
        ArrowBuf outBuffer;
        @Workspace
        nl.basjes.parse.useragent.UserAgentAnalyzer uaa;
        @Workspace
        List<String> allFileds;
        public void setup() {
            uaa = com.dalong.udf.UserAgentAnalyzerProvider.getInstance();
            allFileds= java.util.Arrays.asList("DeviceClass","DeviceName","DeviceBrand","DeviceCpu","OperatingSystemClass","OperatingSystemName","OperatingSystemVersion","OperatingSystemNameVersion","LayoutEngineClass","LayoutEngineName","LayoutEngineVersion","LayoutEngineVersionMajor","LayoutEngineNameVersion","LayoutEngineNameVersionMajor","AgentClass","AgentName","AgentVersion","AgentVersionMajor","AgentNameVersion","AgentNameVersionMajor");
        }
        public void eval() {
            org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter queryMapWriter = outWriter.rootAsStruct();
            if (input.isSet == 0) {
                // Return empty map
                queryMapWriter.start();
                queryMapWriter.end();
                return;
            }
            String userAgentString = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(0,input.end, input.buffer);
            nl.basjes.parse.useragent.UserAgent agent = uaa.parse(userAgentString);
            queryMapWriter.start();
            for (String fieldName : allFileds){
                org.apache.arrow.vector.holders.VarCharHolder rowHolder = new org.apache.arrow.vector.holders.VarCharHolder();
                String field = agent.getValue(fieldName);
                byte[] rowStringBytes = field.getBytes();
                outBuffer.reallocIfNeeded(rowStringBytes.length);
                outBuffer.setBytes(0, rowStringBytes);
                rowHolder.start = 0;
                rowHolder.end = rowStringBytes.length;
                rowHolder.buffer = outBuffer;
                queryMapWriter.varChar(fieldName).write(rowHolder);
            }
            queryMapWriter.end();
        }
    }
    public static class UAGenOutput implements OutputDerivation {
        public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
            return new CompleteType(
                    ArrowType.Struct.INSTANCE,
                    CompleteType.VARCHAR.toField("DeviceClass"),
                    CompleteType.VARCHAR.toField("DeviceName"),
                    CompleteType.VARCHAR.toField("DeviceBrand"),
                    CompleteType.VARCHAR.toField("DeviceCpu"),
                    CompleteType.VARCHAR.toField("OperatingSystemClass"),
                    CompleteType.VARCHAR.toField("OperatingSystemName"),
                    CompleteType.VARCHAR.toField("OperatingSystemVersion"),
                    CompleteType.VARCHAR.toField("OperatingSystemNameVersion"),
                    CompleteType.VARCHAR.toField("LayoutEngineClass"),
                    CompleteType.VARCHAR.toField("LayoutEngineName"),
                    CompleteType.VARCHAR.toField("LayoutEngineVersion"),
                    CompleteType.VARCHAR.toField("LayoutEngineVersionMajor"),
                    CompleteType.VARCHAR.toField("LayoutEngineNameVersion"),
                    CompleteType.VARCHAR.toField("LayoutEngineNameVersionMajor"),
                    CompleteType.VARCHAR.toField("AgentClass"),
                    CompleteType.VARCHAR.toField("AgentName"),
                    CompleteType.VARCHAR.toField("AgentVersion"),
                    CompleteType.VARCHAR.toField("AgentVersionMajor"),
                    CompleteType.VARCHAR.toField("AgentNameVersion"),
                    CompleteType.VARCHAR.toField("AgentNameVersionMajor"));
        }
    }
}

