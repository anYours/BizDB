package com.common.util;

/**
 * @Author wul
 * @Description sql工具
 * @Date 2019/12/11 11:06
 */
public class SQLUtil {

    /**
     * 防止sql注入
     *
     * @param sql
     * @return
     */
    public static String transactSQLInjection(String sql) {
        return sql.replaceAll(".*([';]+|(--)+).*", " ");
    }



    /**
     * 等于号
     * equal
     */
    public static final String  EQ = " = ";

    /**
     * 等于号和问号
     * equal question mark
     */
    public static final String  EQM = " =? ";
    /**
     * 大于号
     * greater-than character
     */
    public static  final String  GT = " > ";

    public static  final String  GTM = " >? ";

    /**
     * 大于等于
     */
    public static  final String  GTE = " >= ";

    public static  final String  GTEM = " >=? ";


    /**
     * 小于号
     * less-than  character
     */
    public static  final String  LT = " < ";

    public static  final String  LTM = " <? ";

    /**
     * 小于等于
     */
    public static  final String  LTE = " <= ";

    public static  final String  LTEM = " <=? ";

    /**
     * 不等于号
     * not equal
     */
    public static  final String  NEQ = " <> ";
    public static  final String  NEQM = " <> ? ";
    /**
     * 恒不等于
     * always not equal
     */
    public static  final String  ALWAYSNEQ = " 1<>1 ";

    /**
     * 左半括号
     * Left parenthesis
     */
    public static  final String  LP = " ( ";

    /**
     * 右半括号
     * Right parenthesis
     */
    public static  final String  RP = " ) ";

    /**
     * 逗号
     * comma
     */
    public static  final String  C = " , ";

    /**
     * 单引号
     * single quotes
     */
    public static  final String  SQ = "'";

    public static  final String  SQC = "','";


    public static  final String  IN = " IN ";

    public static  final String  AND = " AND ";

    public static  final String  ON = " ON ";

    public static  final String  WHERE = " WHERE ";

    public static  final String  OR = " OR ";

    public static  final String  IS_NULL = " IS NULL ";

    public static  final String  IS_NOT_NULL = " IS NOT NULL ";

    public static  final String  NOT_IN = " NOT IN ";

    public static  final String  FROM = " FROM ";

    public static  final String  SELECT = " SELECT ";

    public static  final String  LIKE = " LIKE ";

    public static  final String  LIKEM = " LIKE ? ";

    public static final String COUNT =" COUNT ";

    /**
     * 左单引号
     * '
     */
    public static  final String  LM = " '";

    /**
     * 右单引号
     * '
     */
    public static  final String  RM = "' ";

    /**
     * 百分号
     * %
     * percent
     */
    char PERCENT = '%';
    /**
     * 左连接
     */
    public static  final String  LEFT_JOIN = " LEFT JOIN ";

    /**
     * 右连接
     */

    public static  final String  RIGHT_JOIN = " RIGHT JOIN ";

    /**
     * 内连接
     */

    public static  final String  INNER_JOIN = " INNER JOIN ";
}
