package util;

/**
 * Created by hzliulongfei on 2017/10/13/0013.
 */
public class HashFunc {
    public static int BKDRHash(String str){
        int seed = 131;
        int hash = 0;
        for(int i=0; i<str.length(); i++){
            hash = (hash * seed) + str.charAt(i);
        }
        // 保证返回的hash是正数
        return (hash & 0x7FFFFFFF);
    }
}
