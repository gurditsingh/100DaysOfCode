package com.learn.day1.bloomfilter;

import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {

    @Test
    public void itShouldCheckAttributes(){

        Random random=new Random();
        int bitSize=random.nextInt(20)+1;
        int numberOfElements=random.nextInt(10000)+1;
        int nunberOfHashFunctions=random.nextInt(10)+1;
        BloomFilter<String> bloomFilter=new BloomFilter<>(bitSize,numberOfElements,nunberOfHashFunctions);

        assertEquals(bloomFilter.getExpectedNumberOfElements(),numberOfElements);
        assertEquals(bloomFilter.getNumberOfHashFunction(),nunberOfHashFunctions);
        assertEquals(bloomFilter.getBitSetSize(),bitSize);
    }

    @Test
    public void itShouldCheckHashes(){
        byte[] bytes = "testing".getBytes(Charset.forName("UTF-8"));
        int numHash=10;

        int[] hashes = BloomFilter.createHashes(bytes, numHash);
        assertEquals(hashes.length,numHash);

    }

    @Test
    public void itShouldCheckAdd(){
        BloomFilter<String> bloomFilter=new BloomFilter<>(100,100000,20);
        for(int i=0;i<100;i++){
            String data=UUID.randomUUID().toString();
            bloomFilter.add(data);
        }

        assertEquals(bloomFilter.count(),100);
    }

    @Test
    public void itShouldCheckContains(){
        BloomFilter<String> bloomFilter=new BloomFilter<>(100,100000,20);
        for(int i=0;i<100;i++){
            String data=UUID.randomUUID().toString();
            bloomFilter.add(data);
            assertTrue(bloomFilter.contains(data));
        }

    }

    @Test
    public void itShouldCheckClear(){
        BloomFilter<String> bloomFilter=new BloomFilter<>(100,100000,20);
        for(int i=0;i<100;i++){
            String data=UUID.randomUUID().toString();
            bloomFilter.add(data);
        }

        bloomFilter.clear();
        assertEquals(bloomFilter.count(),0);
    }

    @Test
    public void itShouldCheckEquals(){
        BloomFilter<String> bloomFilter1=new BloomFilter<>(100,100000,20);
        BloomFilter<String> bloomFilter2=new BloomFilter<>(100,100000,20);

        for(int i=0;i<100;i++){
            String data=UUID.randomUUID().toString();
            bloomFilter1.add(data);
            bloomFilter2.add(data);
        }

        assertTrue(bloomFilter1.equals(bloomFilter2));
    }

    @Test
    public void itShouldCheckHashCode(){
        BloomFilter<String> bloomFilter1=new BloomFilter<>(100,100000,20);
        BloomFilter<String> bloomFilter2=new BloomFilter<>(100,100000,20);

        for(int i=0;i<100;i++){
            String data=UUID.randomUUID().toString();
            bloomFilter1.add(data);
            bloomFilter2.add(data);
        }

        assertTrue(bloomFilter1.hashCode() == bloomFilter2.hashCode());
    }

}
