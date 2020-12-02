package three;

public class PageRankDriver {
	public static void main(String[] args) throws Exception {
		int times = 10;

		String[] forGB = { "hdfs://localhost:9000/three/DataSet.txt", "hdfs://localhost:9000/three/output/Data0" };
		GraphBuilder.main(forGB);

		String[] forItr = { "", "" };
		for (int i = 0; i < times; i++) {
			forItr[0] = "hdfs://localhost:9000/three/output/Data" + i;
			forItr[1] = "hdfs://localhost:9000/three/output/Data" + String.valueOf(i + 1);
			PageRankIter.main(forItr);
		}

		String[] forRV = { "hdfs://localhost:9000/three/output/Data" + times,
				"hdfs://localhost:9000/three/output" + "/FinalRank" };
		PageRankViewer.main(forRV);
	}
}
