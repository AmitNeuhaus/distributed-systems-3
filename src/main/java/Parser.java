import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Parser {
    public static List<PatternNoun> parse(String sentence) {
        String[] words = sentence.split(" ");
        String[] tree;
        for (String word : words) {
            String[] recordInNgram = word.split("/");
            String wordItself = wordInNgram[0];
            String tag = wordInNgram[1];
            String depLabel = wordInNgram[2];
            String headIndex = wordInNgram[3];
        }
        return new ArrayList<>();
    }
}
