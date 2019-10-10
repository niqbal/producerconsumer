package queue;

public interface Document {
    String getShardingKey();
    String createDocumentJson();
}
