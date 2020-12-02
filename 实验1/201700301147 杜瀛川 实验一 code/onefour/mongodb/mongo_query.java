package mongodb;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.eq;

public class mongo_query {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// 实例化一个mongo客户端
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		// 实例化一个mongo数据库
		MongoDatabase mongoDatabase = mongoClient.getDatabase("student");
		// 获取数据库中某个集合
		MongoCollection<Document> collection = mongoDatabase.getCollection("student");
		// 进行数据查找,查询条件为name=scofield, 对获取的结果集只显示score这个域
		MongoCursor<Document> cursor = collection.find(new Document("name", "scofield"))
				.projection(new Document("score", 1).append("_id", 0)).iterator();
		while (cursor.hasNext())
			System.out.println(cursor.next().toJson());
	}

}
