package utils;

import java.util.List;

/**
 * Created by rafaelpossas on 13/04/16.
 */
public class Utils {

    public static boolean contains(List<String> list, String string){
        for(String tmp: list){
            if(tmp.equals(string)){
                return true;
            }
        }
        return false;
    }
}
