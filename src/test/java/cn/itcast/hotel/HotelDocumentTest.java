package cn.itcast.hotel;

import cn.itcast.hotel.constants.HotelConstants;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.crypto.spec.PSource;
import java.io.IOException;
import java.util.List;


@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService iHotelService;

    private RestHighLevelClient client;
    @Test
    void testAddDocument() throws IOException {
        // 根据id查询酒店数据
        Hotel hotel = iHotelService.getById(61083L);
        //转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);


        //1.创建request请求
        IndexRequest indexRequest = new IndexRequest("hotel").id(hotel.getId().toString());
        //2.准备Json文档
        indexRequest.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //3.发送请求
        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    /**
     * 文档的查找
     * @throws IOException
     */
    @Test
    void testGetDocumentById() throws IOException {
        //1.准备Request
        GetRequest request = new GetRequest("hotel","61083");
        //2.发送请求，得到响应
        GetResponse getResponse = client.get(request,RequestOptions.DEFAULT);
        //3.解析响应结果
        String json = getResponse.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json,HotelDoc.class);
        System.out.println(hotelDoc);
    }

    /**
     * 文档的更新
     * @throws IOException
     */
    @Test
    void testUpdateDocumentById() throws IOException {
        //1.准备request
        UpdateRequest request = new UpdateRequest("hotel","61083");
        //2.准备请求参数
        request.doc(
          "price","666",
          "starName","四钻石"
        );
        //3.发送请求
        client.update(request,RequestOptions.DEFAULT);
    }

    /**
     * 删除文档
     * @throws IOException
     */
    @Test
    void testDeleteDocument() throws IOException {
        //1.准备Request
        DeleteRequest request = new DeleteRequest("hotel","61083");

        //3.发送请求
        client.delete(request,RequestOptions.DEFAULT);
    }

    //soutagnaog

    @Test
    void testBulkRequest() throws IOException {
        //查询酒店的数据
        List<Hotel> hotels = iHotelService.list();
        //转换为文档类别HotelDoc
        //1.创建Request
        BulkRequest request = new BulkRequest();
        //2.准备参数，添加多个新增的Request
        for(Hotel hotel : hotels){
            HotelDoc hotelDoc = new HotelDoc();
            //创建新增文档的Request对象
            request.add(new IndexRequest("hotel").id(hotel.getId().toString()).
                    source(JSON.toJSONString(hotel),XContentType.JSON));
        }
        //发送请求
        client.bulk(request,RequestOptions.DEFAULT);
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
