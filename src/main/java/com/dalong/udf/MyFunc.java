package com.dalong.udf;


import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import javax.inject.Inject;
import java.util.UUID;

@FunctionTemplate(names = {"myuuid"},desc = "myuuid",scope = FunctionTemplate.FunctionScope.SIMPLE,nulls = FunctionTemplate.NullHandling.NULL_IF_NULL )
public class MyFunc implements SimpleFunction {

    @Param
    NullableVarCharHolder input;
    @Output
    VarCharHolder myout;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
        buffer.clear();
        String uuid = java.util.UUID.randomUUID().toString();
        buffer.setBytes(0, uuid.getBytes());
        myout.buffer = buffer;
        myout.start=0;
        myout.end= uuid.length();
    }

}
