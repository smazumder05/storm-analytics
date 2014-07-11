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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author vagrant
 */
public class WordNormalizer implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void cleanup() {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext ctx, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    /**
     * Tuple here is an array of strings/words from each line
     */
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                //Emit the word
                List ls = new ArrayList();
                ls.add(input);
                collector.emit(ls, new Values(word)); 
            }

        }
        //Acknowledge the input
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
