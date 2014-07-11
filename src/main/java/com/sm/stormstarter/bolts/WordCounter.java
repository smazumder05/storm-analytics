 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.sm.stormstarter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author vagrant
 */
public class WordCounter implements IRichBolt {
    
    Integer id;
    String name;
    Map<String,Integer> counters;
    private OutputCollector collector;
    
    /**
     * At the end of this spout, the word counters will be printed out.
     * 
     */
    @Override
    public void cleanup() {
        System.out.println("Output from Word Counter [" + name + " - " + id + "]---");
        for(Map.Entry<String,Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext ctx, OutputCollector collector) {
        this.counters = new HashMap<String,Integer>();
        this.collector = collector;
        this.name = ctx.getThisComponentId();
        this.id = ctx.getThisTaskId();
    }
    
    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        /**
         * If the word is not found in the map then insert it
         * otherwise increment its count.
         */
        if(!counters.containsKey(str)) {
            counters.put(str, 1);
            
        }else {
            Integer count = counters.get(str) + 1;
            counters.put(str, count);
        }
        //Acknowledge the tuple
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }
}
