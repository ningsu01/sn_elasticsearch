package com.sn.es.config;
 
 
import com.sn.es.util.PropertySourceUtil;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * EliasticSearch连接池工厂对象
 * @author suning
 *
 */
public class EsClientPoolFactory implements PooledObjectFactory<RestHighLevelClient> {

	private static String host = PropertySourceUtil.getPropertyField("elasticsearch_ip","es.properties");

	private static int port = Integer.valueOf(PropertySourceUtil.getPropertyField("elasticsearch_port","es.properties"));

	private static String scheme = PropertySourceUtil.getPropertyField("elasticsearch_scheme","es.properties");


	@Override
	public void activateObject(PooledObject<RestHighLevelClient> arg0) throws Exception {
		System.out.println("activateObject");
		
	}
	
	/**
	 * 销毁对象
	 */
	@Override
	public void destroyObject(PooledObject<RestHighLevelClient> pooledObject) throws Exception {
		RestHighLevelClient highLevelClient = pooledObject.getObject();
		highLevelClient.close();
	}
	
	/**
	 * 生产对象
	 */
//	@SuppressWarnings({ "resource" })
	@Override
	public PooledObject<RestHighLevelClient> makeObject() throws Exception {
//		Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
		RestHighLevelClient client = null;
		try {
			/*client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));*/
			client = new RestHighLevelClient(RestClient.builder(
					new HttpHost(host, port, scheme)));
					/*new HttpHost("192.168.1.123", 9200, "http"), new HttpHost("192.168.1.125", 9200, "http"),
					new HttpHost("192.168.1.126", 9200, "http"), new HttpHost("192.168.1.127", 9200, "http"))*/
 
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new DefaultPooledObject<RestHighLevelClient>(client);
	}
 
	@Override
	public void passivateObject(PooledObject<RestHighLevelClient> arg0) throws Exception {
		System.out.println("passivateObject");
	}
 
	@Override
	public boolean validateObject(PooledObject<RestHighLevelClient> arg0) {
		return true;
	}

}