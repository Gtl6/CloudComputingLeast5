import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 
  
public class bottom_5_Movies_Mapper extends Mapper<Object, 
                            Text, Text, LongWritable> { 
  
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
    public void map(Object key, Text value, 
       Context context) throws IOException,  
                      InterruptedException 
    { 
  
        // input data format => movie_name     
        // no_of_views  (tab seperated) 
        // we split the input data 
        String[] tokens = value.toString().split("\t"); 
  
        String movie_name = tokens[0]; 
        long no_of_views = Long.parseLong(tokens[1]); 
  
        // insert data into treeMap, 
        // we want bottom 5 used words 
        // so we pass no_of_views as key 
        tmap.add(new StringMovieCombiner(no_of_views, movie_name)); 
  
        // we remove the last key-value 
        // if it's size is larget than 5
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
  
            context.write(new Text(name), new LongWritable(count)); 
        } 
    } 
} 