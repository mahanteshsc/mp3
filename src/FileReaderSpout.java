
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private String  filePathStr;
  private BufferedReader reader;
  private Random _rand;
  private AtomicLong linesRead;

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */

    this.context = context;
    this._collector = collector;
    _rand = new Random();
    linesRead = new AtomicLong(0);
    try {
      String fileName= (String) conf.get("linespout.file");
      reader = new BufferedReader(new FileReader(fileName));
      // read and ignore the header if one exists
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
    Utils.sleep(100);

    try {
      String line = reader.readLine();
      if (line != null) {
        long id = linesRead.incrementAndGet();
        _collector.emit(new Values(line), id);
      } else {
        System.out.println("Finished reading file, " + linesRead.get() + " lines read");
        Thread.sleep(10000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
    try {
      reader.close();
    } catch (IOException e) {
        System.out.println("Problem closing file");
    }

  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
    try {
      reader.close();
    } catch (IOException e) {
      System.out.println("Problem closing file");
    }
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
