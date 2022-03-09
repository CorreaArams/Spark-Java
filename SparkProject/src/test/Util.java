package test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Util
{
    public static final Set<String> borings = new HashSet<>();

    static {
        InputStream is = Util.class.getResourceAsStream("/resources/boringwords.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(Objects.requireNonNull(is)));
        br.lines().forEach(borings::add);
    }

    /**
     * Returns true if we think the word is "boring" - ie it doesn't seem to be a keyword
     * for a training course.
     */
    public static boolean isBoring(String word)
    {
        return borings.contains(word);
    }

    /**
     * Convenience method for more readable client code
     */
    public static boolean isNotBoring(String word)
    {
        return !isBoring(word);
    }

}
