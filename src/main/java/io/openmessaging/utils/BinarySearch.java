package io.openmessaging.utils;

import java.util.ArrayList;

public class BinarySearch {

    public static int binarySearch(ArrayList<int[]> indexes, long offset){
        int low = 0;
        int middle = 0;
        int high = indexes.size() - 1;

        while(low <= high){
            middle = (low + high) / 2;
            if(indexes.get(middle)[0] > offset){
                high = middle - 1;
            }else if(indexes.get(middle)[0] < offset){
                low = middle + 1;
            } else {
                high = middle;
                break;
            }
        }

        return high;
    }

}
