package com.lyj;


import com.lyj.util.ConfigLoader;

import static com.lyj.util.TableUtil.executeSelectSql;


public class Test {
    public static void main(String[] args) throws Exception {

        ConfigLoader.loadConfiguration("dev43");
        String str = "select count(1) from xuexiaodingtest.i03n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.i28f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.i34n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.k39za_prod_db      \n" +
                "select count(1) from xuexiaodingtest.m03z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.m04_prod_db        \n" +
                "select count(1) from xuexiaodingtest.m04r_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r02n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r03k_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r051n_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r052z_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r061_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r062f_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r066_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r066f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r066k_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r066n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r066z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r26z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r31_prod_db        \n" +
                "select count(1) from xuexiaodingtest.i03k_prod_db       \n" +
                "select count(1) from xuexiaodingtest.i28n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.i28z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.i34_prod_db        \n" +
                "select count(1) from xuexiaodingtest.k39fa_prod_db      \n" +
                "select count(1) from xuexiaodingtest.k39ka_prod_db      \n" +
                "select count(1) from xuexiaodingtest.m03f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.m03k_prod_db       \n" +
                "select count(1) from xuexiaodingtest.m04k_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r02k_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r03f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r051k_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r051z_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r052f_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r061k_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r062_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r062k_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r062n_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r26f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r26n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r31f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r31n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.u11z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.i03_prod_db        \n" +
                "select count(1) from xuexiaodingtest.i03f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.i03z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.k39_prod_db        \n" +
                "select count(1) from xuexiaodingtest.m03_prod_db        \n" +
                "select count(1) from xuexiaodingtest.m03r_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r02_prod_db        \n" +
                "select count(1) from xuexiaodingtest.r03z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r051_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r052k_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r061f_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r26_prod_db        \n" +
                "select count(1) from xuexiaodingtest.r91_prod_db        \n" +
                "select count(1) from xuexiaodingtest.i09n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.i28_prod_db        \n" +
                "select count(1) from xuexiaodingtest.i34z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.k39na_prod_db      \n" +
                "select count(1) from xuexiaodingtest.m03n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.m04f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.m04n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.m04z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r02f_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r02z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r03_prod_db        \n" +
                "select count(1) from xuexiaodingtest.r03n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r051f_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r052_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r052n_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r061n_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r061z_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r062z_prod_db      \n" +
                "select count(1) from xuexiaodingtest.r26k_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r31k_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r31z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r73z_prod_db       \n" +
                "select count(1) from xuexiaodingtest.r75_prod_db        \n" +
                "select count(1) from xuexiaodingtest.r90_prod_db        \n" +
                "select count(1) from xuexiaodingtest.u11_prod_db        \n" +
                "select count(1) from xuexiaodingtest.u11n_prod_db       \n" +
                "select count(1) from xuexiaodingtest.m32_prod_db        \n" +
                "select count(1) from xuexiaodingtest.u16_prod_db        \n" +
                "select count(1) from xuexiaodingtest.u15_prod_db        ";

        StringBuilder sb = new StringBuilder();
        for (String sql : str.split("\n")) {
            int count = executeSelectSql(sql);
            sb.append(sql + " ; -- " + count).append("\n");
        }
        System.out.println(sb);

    }
}
