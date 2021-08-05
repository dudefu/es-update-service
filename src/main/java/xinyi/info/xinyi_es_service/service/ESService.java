package xinyi.info.xinyi_es_service.service;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import xinyi.info.xinyi_es_service.config.EsClientFactory;
import xinyi.info.xinyi_es_service.utils.DateUtils;
import xinyi.info.xinyi_es_service.utils.PropertiesUtils;

import java.io.IOException;
import java.util.*;

/**
 * @author ：dudefu
 * @date ：Created in 2021/7/27 10:28
 * @description：
 * @version: $
 */
public class ESService {

    protected static Logger logger = LoggerFactory.getLogger(ESService.class);

    public static void update(String index,String type,String rangeQueryName,String updateField, String updateValue,String oldValue,
                              Date startDate, Date endDate) {

        TransportClient client = EsClientFactory.getInstance();

        SearchResponse response = queryInfo(index,client,rangeQueryName,startDate,endDate);

        // 数据量总数
        if(response != null ){
            long totalHits = response.getHits().getTotalHits();
            String startTime = DateUtils.format(startDate,DateUtils.TO_SECOND);
            String endTime = DateUtils.format(endDate,DateUtils.TO_SECOND);
            logger.info("index:[{}],startDate:[{}],endDate:[{}],数据量总数:[{}]" ,index,startTime,endTime,totalHits);
            if(totalHits != 0 ){
                int runNum = 0;
                int hasNum = 0 ;
                logger.info("开始处理index:[{}];总数：[{}] ;时间：[{}] ", index, runNum + "/" + hasNum, System.currentTimeMillis());

                do {
                    SearchHits hits = response.getHits();
                    for (SearchHit searchHit : hits) {
//                        logger.info("正在处理index:[{}];进度：[{}];时间：[{}]  ", index, runNum + "/" + totalHits, DateUtils.format(DateUtils.getNowTime(), DateUtils.TO_SECOND));
                        runNum++;
                        Map<String, Object> source = searchHit.getSource();
                        Map<String,Object> updateFieldMap = new HashMap<>();
                        String id = searchHit.getId();
                        String[] updateFieldArray = updateField.split(",");
                        for (String updateFieldValue : updateFieldArray) {
                            String oldValues = (String) source.get(updateFieldValue);
                            if (oldValues != null && oldValues.contains(oldValue)) {
                                updateFieldMap.put(updateFieldValue,oldValues.replace(oldValue,updateValue));
                            }
                        }
                        if(updateFieldMap.size() > 0){
                            logger.info("正在处理index:[{}];进度：[{}];时间：[{}]  ", index, runNum + "/" + totalHits, DateUtils.format(DateUtils.getNowTime(), DateUtils.TO_SECOND));
                            Boolean b = updateEsBatchUrl(client, type, index, id, updateFieldMap);
                            if (b) {
                                logger.info("===> index:[{}],id:[{}]更新成功" ,index,id );
                                hasNum++;
                            } else {
                                logger.info("===> index:[{}],id:[{}]更新失败" ,index,id );
                            }
                        }
//                        else{
//                            logger.info("===> index:[{}],id:[{}]无可更新数据" ,index,id );
//                        }
                    }
                    // 重新获取数据
                    response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60*60000))
                            .execute().actionGet();
                    // 判断是否有数据
                }while(response.getHits().getHits().length != 0);
            }
        }

    }

    /**
     * 查询结果集
     * @param index
     * @param client
     * @param rangeQueryName
     * @param start
     * @param end
     * @return
     */
    public static SearchResponse queryInfo(String index, Client client,String rangeQueryName, Date start, Date end) {
        // 查询结果
        SearchResponse response = null;
        try
        {
            BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery()
                    .must(QueryBuilders.rangeQuery(rangeQueryName).gt(start).lt(end));
            SearchRequestBuilder searchBuilder = client.prepareSearch(index)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .addSort(rangeQueryName, SortOrder.ASC)
                    .setScroll(new TimeValue(60 * 60000)).setSize(1000);

            response = searchBuilder.setQuery(booleanQuery).execute().actionGet();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return response;
    }

    /**
     * 根据_id更新数据
     * @param client
     * @param type
     * @param index
     * @param id
     * @param updateFieldMap
     * @return
     */
    public static boolean updateEsBatchUrl(Client client, String type, String index,String id, Map<String,Object> updateFieldMap) {

        boolean r = true;
        try {

            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (Map.Entry<String, Object> entry : updateFieldMap.entrySet()){
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            bulkRequestBuilder.add(client.prepareUpdate(index,type,id).setDoc(builder));
            BulkResponse bulkResponse = bulkRequestBuilder.get();

            if (bulkResponse.hasFailures()) {
                logger.error(bulkResponse.buildFailureMessage());
                logger.error("存在失败操作");
                r = false;
            }

        } catch (IOException e) {
            r = false;
            e.printStackTrace();
        }
        return r;
    }
}
