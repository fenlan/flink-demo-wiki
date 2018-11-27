package com.fenlan.flink.sougou;

import com.fenlan.flink.twitter.TwitterAnalysis;
import com.hankcs.hanlp.HanLP;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.*;

public class LogRead {

    private static Log log = LogFactory.getLog(LogRead.class);

    public static void main(String[] args) throws IOException {
        Map<String, Long> negMap = new HashMap<>();
        Map<String, Long> posMap = new HashMap<>();
        Set<String> negTest = new HashSet<>(2000);
        Set<String> posTest = new HashSet<>(2000);
        File file = new File("/home/fenlan/Downloads/chinese_data/hotel/neg.txt");
        InputStreamReader reader = new InputStreamReader(new FileInputStream(file), "GBK");
        BufferedReader bufferedReader = new BufferedReader(reader);

        String line = null;
        long i = 0;
        while ((line = bufferedReader.readLine()) != null) {
            if (i++ < 4000) {
                List<String> list = HanLP.extractKeyword(line, 30);
                for (String word : list) {
                    if (negMap.containsKey(word))
                        negMap.put(word, negMap.get(word) + 1);
                    else
                        negMap.put(word, 1L);
                }
            }
            else
                negTest.add(line);
        }

        reader.close();
        bufferedReader.close();

        file = new File("/home/fenlan/Downloads/chinese_data/hotel/pos.txt");
        InputStreamReader reader1 = new InputStreamReader(new FileInputStream(file), "GBK");
        BufferedReader bufferedReader1 = new BufferedReader(reader1);

        line = null;
        i = 0;
        while ((line = bufferedReader1.readLine()) != null) {
            if (i++ < 4000) {
                List<String> list = HanLP.extractKeyword(line, 10);
                for (String word : list) {
                    if (posMap.containsKey(word))
                        posMap.put(word, posMap.get(word) + 1);
                    else
                        posMap.put(word, 1L);
                }
            }
            else
                posTest.add(line);
        }

        reader1.close();
        bufferedReader1.close();

        long right = 0, wrong = 0, tmp = 0;
        double p1 = 1.0, p2 = 1.0;
        for (String sentence : negTest) {
            List<String> list = HanLP.extractKeyword(sentence, 10);
            for (String word : list) {
                tmp = negMap.get(word) == null ? 1 : negMap.get(word);
                p1 *= (double) tmp / negMap.size();
                tmp = posMap.get(word) == null ? 1 : posMap.get(word);
                p2 *= (double)tmp / posMap.size();
            }
            tmp = p1 >= p2 ? right++ : wrong++;
            p1 = 1;
            p2 = 1;
        }
        for (String sentence : posTest) {
            List<String> list = HanLP.extractKeyword(sentence, 10);
            for (String word : list) {
                tmp = negMap.get(word) == null ? 1 : negMap.get(word);
                p1 *= (double) tmp / negMap.size();
                tmp = posMap.get(word) == null ? 1 : posMap.get(word);
                p2 *= (double) tmp / posMap.size();
            }
            tmp = p1 <= p2 ? right++ : wrong++;
            p1 = 1;
            p2 = 1;
        }

        System.out.println((double)right / (right+wrong));
    }
}
