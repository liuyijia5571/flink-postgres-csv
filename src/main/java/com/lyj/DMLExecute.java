package com.lyj;


import com.lyj.util.ConfigLoader;

import static com.lyj.util.TableUtil.executeSelectSql;

/**
 * 执行DML 语句在指定的数据库中
 */
public class DMLExecute {
    public static void main(String[] args) throws Exception {

        ConfigLoader.loadConfiguration("dev43");
        String str = "select count(1) from xuexiaodingtest2.i03n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.i28f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.i34n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.k39za_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.m03z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.m04_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.m04r_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r02n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r03k_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r051n_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r052z_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r061_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r062f_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r066_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r066f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r066k_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r066n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r066z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r26z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r31_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.i03k_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.i28n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.i28z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.i34_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.k39fa_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.k39ka_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.m03f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.m03k_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.m04k_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r02k_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r03f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r051k_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r051z_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r052f_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r061k_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r062_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r062k_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r062n_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r26f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r26n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r31f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r31n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.u11z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.i03_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.i03f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.i03z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.k39_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.m03_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.m03r_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r02_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.r03z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r051_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r052k_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r061f_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r26_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.r91_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.i09n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.i28_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.i34z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.k39na_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.m03n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.m04f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.m04n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.m04z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r02f_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r02z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r03_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.r03n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r051f_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r052_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r052n_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r061n_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r061z_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r062z_prod_db      \n" +
                "select count(1) from xuexiaodingtest2.r26k_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r31k_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r31z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r73z_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.r75_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.r90_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.u11_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.u11n_prod_db       \n" +
                "select count(1) from xuexiaodingtest2.m32_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.u16_prod_db        \n" +
                "select count(1) from xuexiaodingtest2.u15_prod_db        ";

        StringBuilder sb = new StringBuilder();
        for (String sql : str.split("\n")) {
            int count = executeSelectSql(sql);
            sb.append(sql).append(" ; -- ").append(count).append("\n");
        }
        System.out.println(sb);

    }
}
