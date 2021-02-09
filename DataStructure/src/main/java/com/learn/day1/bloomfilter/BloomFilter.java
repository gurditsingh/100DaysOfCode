package com.learn.day1.bloomfilter;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.BitSet;
import java.util.Objects;

public class BloomFilter<T> implements Serializable {

    private BitSet bitSet;
    private int bitSetSize;
    private int expectedNumberOfElements;
    private int numberOfHashFunction;
    private long numberOfTotalElements=0L;

    final private Charset charset = Charset.forName("UTF-8");

    public BloomFilter(int bitSetSize, int numberOfElements, int numberOfHashFunction){
        this.expectedNumberOfElements=numberOfElements;
        this.numberOfHashFunction=numberOfHashFunction;
        this.bitSetSize=bitSetSize;
        this.bitSet=new BitSet(bitSetSize);
    }

    public static int[] createHashes(byte[] data, int hashes) {
        int[] result = new int[hashes];

        int k = 0;
        while (k < hashes) {
            byte[] digest= DigestUtils.md5(data);

            for (int i = 0; i < digest.length/4 && k < hashes; i++) {
                int h = 0;
                for (int j = (i*4); j < (i*4)+4; j++) {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }


    public void add(T element){
        add(element.toString().getBytes(charset));
    }

    public void add(byte[] bytes){
        int[] hashes = createHashes(bytes, numberOfHashFunction);
        for(int hash:hashes){
            bitSet.set(Math.abs(hash % bitSetSize), true);
        }
        numberOfTotalElements+=1;
    }

    public boolean contains(T element){
        return contains(element.toString().getBytes(charset));
    }

    public boolean contains(byte[] bytes){
        int[] hashes = createHashes(bytes, numberOfHashFunction);
        for(int hash:hashes) {
            if(!bitSet.get(Math.abs(hash % bitSetSize)))
                return false;
        }
        return true;
    }

    public void clear() {
        bitSet.clear();
        numberOfTotalElements= 0L;
    }

    public long count() {
        return this.numberOfTotalElements;
    }

    public int getBitSetSize() {
        return bitSetSize;
    }

    public int getExpectedNumberOfElements() {
        return expectedNumberOfElements;
    }

    public int getNumberOfHashFunction() {
        return numberOfHashFunction;
    }

    public long getNumberOfTotalElements() {
        return numberOfTotalElements;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BloomFilter<T> other = (BloomFilter<T>) obj;
        if (this.expectedNumberOfElements != other.expectedNumberOfElements) {
            return false;
        }
        if (this.numberOfHashFunction != other.numberOfHashFunction) {
            return false;
        }
        if (this.bitSetSize != other.bitSetSize) {
            return false;
        }
        if (!Objects.equals(this.bitSet, other.bitSet)) {
            return false;
        }
        return true;
    }


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (this.bitSet != null ? this.bitSet.hashCode() : 0);
        hash = 31 * hash + this.expectedNumberOfElements;
        hash = 31 * hash + this.bitSetSize;
        hash = 31 * hash + this.numberOfHashFunction;
        return hash;
    }





}
