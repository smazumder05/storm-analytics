/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sm.stormstarter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.sm.stormstarter.bolts.WordCounter;
import com.sm.stormstarter.bolts.WordNormalizer;
import com.sm.stormstarter.spouts.WordReader;

/**
 *
 * @author vagrant
 */
public class StormStarterMain {

    public static void main(String[] args) throws InterruptedException  {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                        .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),1)
                       .fieldsGrouping("word-normalizer", new Fields("word"));
        
        //configure 
        Config conf = new Config();
        conf.put("wordsfile", args[0]);
        conf.setDebug(false);
        //Run this topology
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster local = new LocalCluster();
        local.submitTopology("Storm-Starter-Topology", conf, builder.createTopology());
        Thread.sleep(3000);
        local.shutdown();
    }

}
