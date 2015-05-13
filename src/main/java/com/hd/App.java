package com.hd;

import com.hd.crystalBall.ComputeRelativeFrequency;
import com.hd.wordCount.WordCount;


/**
 * Created with IntelliJ IDEA.
 * User: naresh
 * Date: 5/12/15
 * Time: 8:06 AM
 * To change this template use File | Settings | File Templates.
 */
public class App {
    public static void main(String[] args) throws Exception {
        if(args[0].equals("WordCount")){
            WordCount.main(args);
        }
        switch (args[0]){
            case "WordCount":
                WordCount.main(args);
                break;
            case "CrystalBall":
                ComputeRelativeFrequency.main(args);
            default:
                System.out.println("Incorrect "+args[0] +" command.");
        }
    }
}
