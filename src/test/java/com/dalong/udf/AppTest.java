package com.dalong.udf;


import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import org.junit.Test;

import java.math.BigDecimal;

public class AppTest extends BaseTestQuery {
    @Test
    public void invalidLocate() throws Exception {
        thrownException.expect(UserException.class);
        String query = "SELECT parse_user_agent3('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36')";
//        test("SELECT parse_user_agent3()");
        testBuilder().sqlQuery(query)
                .unOrdered()
                .baselineColumns("EXPR$0")
                .baselineValues("")
                .go();
    }

}
