/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.sm.stormstarter.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
/**
 *
 * @author vagrant
 */
public class WordReader implements IRichSpout {
    
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;
    
    @Override
    public boolean isDistributed(){
        return false;
    }
    
    public void ack(Object messageID) {
        System.out.println("Message Processed: " + messageID);
        
    }
    
    @Override
    public void close() {
        
    }
    
    @Override
    public void fail(Object messageID) {
        System.out.println("Message FAILED: " + messageID);
    }
    
    /**
     * the only work that this method does is to 
     */
    @Override
    public void nextTuple() {
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                //Do nothing
            }
            return;
        }
        String line = null;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
             while((line = reader.readLine()) != null ) {
                 this.collector.emit(new Values(line), line);
             }
            
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple.", e);
        } finally {
            completed = true;
        }
        
        
    }
    @Override
    public void open(Map conf, TopologyContext ctx, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("wordsfile").toString());
        } catch(FileNotFoundException fne) {
            throw new RuntimeException("File not found: " + conf.get("wordsfile"));
        }
        this.collector = collector;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
