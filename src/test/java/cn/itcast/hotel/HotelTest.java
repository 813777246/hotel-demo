package cn.itcast.hotel;

import cn.itcast.hotel.constants.HotelConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class HotelTest {
    private RestHighLevelClient client;

    @Test
    void testInit(){
        System.out.println(client);
    }

    /**
     * 创建索引库
     * @throws IOException
     */
    @Test
    void createHotelIndex() throws IOException {
        //1.创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2.准备请求的参数
        request.source(HotelConstants.MAPPING_TEMPLATE, XContentType.JSON);
        //3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引库
     * @throws IOException
     */
    @Test
    void deleteHotelIndex() throws IOException {
        //1.创建request对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");

        //2.发送请求
        client.indices().delete(request,RequestOptions.DEFAULT);
    }

    @Test
    void testExistsHotelIndex() throws IOException {
        //1.创建request对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        //2.发送请求
        boolean exists = client.indices().exists(request,RequestOptions.DEFAULT);
        //3.输出
        System.err.println(exists?"数据库存在":"数据库不存在");
    }
    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.95.129:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException{
        this.client.close();
    }

}
