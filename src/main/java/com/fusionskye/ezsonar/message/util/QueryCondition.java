package com.fusionskye.ezsonar.message.util;

import com.fusionskye.ezsonar.message.model.Stream;
import com.google.common.collect.Lists;
import org.bson.types.BasicBSONList;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.query.*;

import java.util.List;

/**
 * 由于该类只在ES查询时使用， 就放在MessageGateWay内部 用于接收一个条件对应的Map对象， 该Map对象，可以包含多个key。
 * 在多个key的条件下， QueryCondition可以生成一个And类型的QueryFilter，
 */
public class QueryCondition {

    private static final String STREAMS_KEY = "streams";

    /**
     * 根据传入的流id数组， 获取查询成功率所需要的成功数量Filter和所有数量Filter
     *
     * @param streams 对于没有返回码的交易，计算成功率的时候，需要考虑这部分交易是否纳入计算：
     *                <p>
     *                1. 算作成功
     *                分母为所有交易，分子为成功的交易+没有返回码的交易
     *                分子：and[ term{streams:xxxx}, or[ terms {success_ret_code : success_ret_code_values}, missing{"field":success_ret_code}] ]
     *                分母：term{streams:xxxx}
     *                <p>
     *                2. 算作失败
     *                分母为所有交易，分子为成功的交易
     *                分子：and[ term{streams:xxxx},  terms {success_ret_code : success_ret_code_values}]
     *                分母：term{streams: xxxx}
     *                <p>
     *                3. 不纳入计算
     *                分母为（所有交易-没有返回码的交易）， 分子为成功的交易
     *                分子： and[ term{streams:xxxx}, terms {success_ret_code : success_ret_code_values}]
     *                分母： and [ term{streams:xxxx}, exists{"field":success_ret_code}]
     */

    public static
    @Nullable
    FilterBuilder generateSuccessFilterBuilder(Stream... streams) {

        final List<FilterBuilder> successFilterList = Lists.newArrayList();
        if (streams != null) {
            for (Stream stream : streams) {
                if (stream == null) {
                    continue;
                }
                BasicBSONList succeedQueryList = stream.getSuccess_ret_codes();
                if (!(succeedQueryList == null || succeedQueryList.isEmpty())) {
                    List<FilterBuilder> filterList = Lists.newArrayList();
                    for (Object code : succeedQueryList) {
                        QueryStringQueryBuilder qb = QueryBuilders.queryString(code.toString());
                        QueryFilterBuilder fb = FilterBuilders.queryFilter(qb);
                        filterList.add(fb);
                    }
                    FilterBuilder successFilterBuilder = FilterBuilders.boolFilter().must(
                            FilterBuilders.termFilter(STREAMS_KEY, stream.getId()),
                            FilterBuilders.boolFilter().should(filterList.toArray(new FilterBuilder[filterList.size()]))
                    );

                    successFilterList.add(successFilterBuilder);
                } else {
                    successFilterList.add(FilterBuilders.termFilter(STREAMS_KEY, stream.getId()));
                }
            }
        }
        return successFilterList.isEmpty() ? null :
                FilterBuilders.boolFilter().should(successFilterList.toArray(new FilterBuilder[successFilterList.size()]));
    }

    /**
     * 是否设置过成功返回码 , 有一条流设置则为 true
     *
     * @param streams 多条流
     * @return true or false
     */
    public static boolean isSettingSuccessRetCodes(Stream... streams) {

        if (streams == null) {
            return false;
        }
        boolean b = false;
        for (Stream stream : streams) {
            if (stream == null) {
                continue;
            }
            final BasicBSONList successRetCodes = stream.getSuccess_ret_codes();
            b = !(null == successRetCodes || successRetCodes.isEmpty());
            if (b) {
                break;
            }
        }
        return b;
    }


}
