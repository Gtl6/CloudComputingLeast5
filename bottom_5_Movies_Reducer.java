import java.io.IOException;
import java.util.ArrayList;
  
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 
  
public class bottom_5_Movies_Reducer extends Reducer<Text, 
                     LongWritable, LongWritable, Text> { 
  
	private ArrayList<StringMovieCombiner> tmap;
  
    @Override
    public void setup(Context context) throws IOException, 
                                     InterruptedException 
    { 
        tmap = new ArrayList<StringMovieCombiner>(); 
    } 
    
    private void removeLargestFromTmap() {
    	Long largestVal = tmap.get(0).getLong();
    	int pos = 0;
    	
    	// 6 because it's the bottom 5 movies, so our arraylist with 6 elements will go 0 to 5
    	for(int i = 1; i < 6; i++) {
    		if(tmap.get(i).getLong() > largestVal) {
    			pos = i;
    			largestVal = tmap.get(i).getLong();
    		}
    	}
    	
    	tmap.remove(pos);
    }
  
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, 
      Context context) throws IOException, InterruptedException 
    { 
  
        // input data from mapper 
        // key                values 
        // movie_name         [ count ] 
        String name = key.toString(); 
        long count = 0; 
  
        for (LongWritable val : values) 
        { 
            count = val.get(); 
        } 
  
        // insert data into treeMap, 
        // we want top 10 viewed movies 
        // so we pass count as key 
        tmap.add(new StringMovieCombiner(count, name)); 
  
        // we remove the first key-value 
        // if it's size increases 10 
        if (tmap.size() > 5) 
        { 
            removeLargestFromTmap(); 
        } 
    } 
  
    @Override
    public void cleanup(Context context) throws IOException, 
                                       InterruptedException 
    { 
  
        for (StringMovieCombiner smc : tmap)  
        { 
  
            long count = smc.getLong(); 
            String name = smc.getString(); 
            context.write(new LongWritable(count), new Text(name)); 
        } 
    } 
} 