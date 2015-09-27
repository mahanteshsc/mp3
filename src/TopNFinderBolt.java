import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
  private int N;
  Map<String, Integer> counts = new HashMap<String, Integer>();

  private long intervalToReport = 20;
  private long lastReportTime = System.currentTimeMillis();

  public TopNFinderBolt(int N) {
    this.N = N;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words
    ------------------------------------------------- */
      String word = tuple.getString(0);
      Integer count = tuple.getInteger(1);

      if (currentTopWords.size() < this.N && !currentTopWords.containsKey(word)) {
          currentTopWords.put(word, count);
      } else {
//          Iterator it = currentTopWords.entrySet().iterator();
//          while (it.hasNext()) {
//              Map.Entry pair = (Map.Entry)it.next();
//              System.out.println(pair.getKey() + " = " + pair.getValue());
//              it.remove(); // avoids a ConcurrentModificationException
//          }
          for (Map.Entry<String, Integer> entry : currentTopWords.entrySet()) {
              if(entry.getValue()< count){
                  currentTopWords.remove(entry.getKey());
                  currentTopWords.put(word,count);
                  break;
              }
          }
      }

      //reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values(printMap()));
      lastReportTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

     declarer.declare(new Fields("top-N"));

  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
      stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    stringBuilder.append("]");
    return stringBuilder.toString();

  }
}
