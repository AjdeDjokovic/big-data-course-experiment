package mongodb;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class mongo_insert {

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
		// 实例化一个文档,内嵌一个子文档
		Document document = new Document("name", "scofield").append("score",
				new Document("English", 45).append("Math", 89).append("Computer", 100));
		List<Document> documents = new ArrayList<Document>();
		documents.add(document);
		// 将文档插入集合中
		collection.insertMany(documents);
		System.out.println("文档插入成功");
	}

}
