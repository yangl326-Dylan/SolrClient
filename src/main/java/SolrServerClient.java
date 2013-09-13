import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: zhoudylan
 * Date: 13-9-3
 * Time: 上午11:40
 * To change this template use File | Settings | File Templates.
 */
public class SolrServerClient {

    private String SOLR_ADMIN_URL = "http://localhost:8983/solr/collection1";
//    private String SOLR_ADMIN_URL = "http://localhost:8983/solr/";
    private static HttpSolrServer server = null;
    private static SolrServerClient solrServiceClient = null;

    /**
     * log 实例
     */
    private static final Logger log = Logger.getLogger(SolrServerClient.class.getName());

    private SolrServerClient() {
        this.getServer();
    }


    /**
     * SolrServerClient 是线程安全的 需要采用单例模式
     * 此处实现方法适用于高频率调用查询
     *
     * @return SolrServerClient
     */
    public static SolrServerClient getInstance() {
        if (solrServiceClient == null) {
            synchronized (SolrServerClient.class) {
                if (solrServiceClient == null) {
                    solrServiceClient = new SolrServerClient();
                }
            }
        }
        return solrServiceClient;
    }


    /**
     * 初始化的HttpSolrServer 对象,并获取此唯一对象
     * 配置按需调整
     * @return 此server对象
     */
    private HttpSolrServer getServer() {
        if (server == null) {
            server = new HttpSolrServer(SOLR_ADMIN_URL);
            server.setConnectionTimeout(3000);
            server.setDefaultMaxConnectionsPerHost(100);
            server.setMaxTotalConnections(100);
        }
        return server;
    }

    /**
     * 查询方法    例子
     *
     * @param q     查询参数
     * @param start 起始行（缺省请使用0 代表首页结果） 分页使用
     * @param rows  需要显示的行数  分页使用
     * @return 装有hashMap结果的list
     */
    @Deprecated
    public List<Map> query(String q, String name, int start, int rows) {
        HttpSolrServer solrServer = SolrServerClient.getInstance().getServer();
        SolrQuery params = new SolrQuery(q);
        params.set("df", "all");              // 缺省查询field  可以为copy字段  这样比较方便 不用指定key，多字段可混查
        params.set("fq", "name:" + name);     // 限制参数
        params.set("wt", "xml");              // 缺省返回格式
        params.set("start", start);
        params.set("rows", rows);
        List<Map> resultList = new ArrayList<Map>();
        try {
            QueryResponse response = solrServer.query(params);
            SolrDocumentList list = response.getResults();
            log.info(" 查询结果总计: " + list.getNumFound() + "条，本次: " + list.size() + "条。"
                    + "query t: "+response.getQTime()+"ms;  elapsed t: "+response.getElapsedTime() + "ms;");
            for (int i = 0; i < list.size(); i++) {
                Map<String, String> result = new HashMap<String, String>();
                SolrDocument doc = list.get(i);
                result.put("id", String.valueOf(doc.get("id")));
                result.put("name", String.valueOf(doc.get("name")));
                resultList.add(result);
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
            log.warning("Queue error:" + e.getMessage());
        }
        return resultList;
    }


    /**
     * 主要包含通配符禁止、大小写统一、空白符滤除
     * 查询接口预处理
     * @param s 搜索内容
     * @return 处理后的搜索内容
     */
    private static String preHandleQuery(String s) {
        if (s.indexOf("*") > 0) {
            log.info("* in query! ignore query~");
            return "";
        } else {
            s = s.replaceAll("\\s*", "").replace("/", "");
            return s.toLowerCase();
        }
    }


    /**
     * 移除某一key下所有索引数据
     * key=* 表示删除所有索引
     * @param key  某一类key
     */
    public void deleteAllIndex(String key) {
        try {
            HttpSolrServer solrServer = SolrServerClient.getInstance().getServer();
            solrServer.deleteByQuery(key+":*");
            solrServer.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一批索引
     *
     * @param ids unique key的list
     */
    public void deleteIndex(List<String> ids) {
        try {
            HttpSolrServer solrServer = SolrServerClient.getInstance().getServer();
            solrServer.deleteById(ids);
            solrServer.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除某一个索引
     *
     * @param id unique key
     */
    public void deleteIndex(String id) {
        try {
            HttpSolrServer solrServer = SolrServerClient.getInstance().getServer();
            solrServer.deleteById(id);
            solrServer.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * main test
     *
     * @param args
     */
    public static void main(String[] args) {
        long a = System.currentTimeMillis();

        SolrServerClient.getInstance().query("*","*",0,24);

        long b = System.currentTimeMillis();
        System.out.println((b - a));

    }
}

