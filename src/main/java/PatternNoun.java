import java.io.IOException;

public class PatternNoun {
    public String pattern;
    public String noun1;
    public String noun2;

    public PatternNoun( String noun1, String noun2,String pattern,boolean stem) throws IOException {
        if(stem) {
            this.noun1 = Parser.stem(noun1);
            this.noun2 = Parser.stem(noun2);
            this.pattern = Parser.stem(pattern);
        }else {
            this.noun1 = noun1;
            this.noun2 = noun2;
            this.pattern = pattern;
        }
    }
}
