package com.test.spark;

import java.util.ArrayList;
import java.util.List;

public class PrintAllSubArrays {

    public int evenSubarray(List<Integer> numbers, int k){

        int count = 0;
        for (int i=0; i< numbers.size(); i++){
            int odd = 0;

            for (int j=i; j<numbers.size(); j++){

                for (int l = i ; l < j ; l++) {
                    if (numbers.get(l) % 2 ==1)
                        odd++;
                    if (odd <= k)
                        count++;
                }


            }
        }
        return count;
    }

    public static void main(String[] args) {
        List<Integer> al = new ArrayList<Integer>();
        al.add(1);
        al.add(2);
        al.add(3);
        al.add(4);
        int val = new PrintAllSubArrays().evenSubarray(al, 1 );
        System.out.println(val);
    }
}
