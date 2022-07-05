package spring.booktraker.dataloader.booktrakerdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.datastax.oss.driver.shaded.guava.common.base.Optional;

import spring.booktraker.dataloader.booktrakerdataloader.author.Author;
import spring.booktraker.dataloader.booktrakerdataloader.author.AuthorRepository;
import spring.booktraker.dataloader.booktrakerdataloader.book.Book;
import spring.booktraker.dataloader.booktrakerdataloader.book.BookRepository;
import spring.booktraker.dataloader.booktrakerdataloader.connection.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookTrakerDataloaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String workDumpLocation;


	public static void main(String[] args) {
		SpringApplication.run(BookTrakerDataloaderApplication.class, args);
	}

	/**
     * This is necessary to have the Spring Boot app use the Astra secure bundle 
     * to connect to the database
     */
	@Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

	private void initAuthors(){
		Path path = Paths.get(authorDumpLocation);
		try(Stream<String> lines=Files.lines(path)){
			lines.forEach(line -> {
				String jsonString=line.substring(line.indexOf("{"));
				try{
				JSONObject jsonObject=new JSONObject(jsonString);
				Author author = new Author();
				author.setName(jsonObject.optString("name"));
				author.setPersonalName(jsonObject.optString("personal_name"));
				author.setId((jsonObject.optString("key")).replace(("/authors/"),""));
				authorRepository.save(author);
				} catch(JSONException e){
					e.printStackTrace();
				}
			});
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	private void initWorks(){
		Path path = Paths.get(workDumpLocation);
		DateTimeFormatter dateFormat=DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines=Files.lines(path)){
			lines.forEach(line -> {
				String jsonString=line.substring(line.indexOf("{"));
				try{
					JSONObject jsonObject=new JSONObject(jsonString);
					Book book = new Book();
					book.setId((jsonObject.getString("key")).replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					JSONObject descriptioObject = jsonObject.optJSONObject("description");
					if(descriptioObject!=null){
						book.setDescription(descriptioObject.optString("value"));
					}
					JSONObject publishedObject = jsonObject.optJSONObject("created");
					if(publishedObject!=null){
						String datestr = publishedObject.getString("value");
						book.setPublishedDate(LocalDate.parse(datestr,dateFormat));
					}
					JSONArray coversJsonArray=jsonObject.optJSONArray("covers");
					if(coversJsonArray!=null){
						List<String> coverIds = new ArrayList<>();
						for(int i=0;i<coversJsonArray.length();i++){
							coverIds.add(String.valueOf(coversJsonArray.get(i)));
						}
						book.setCoverIds(coverIds);
					}
					JSONArray authorsJsonArray=jsonObject.optJSONArray("authors");
					if(authorsJsonArray!=null){
						List<String> authorIds = new ArrayList<>();
						for(int i=0;i<authorsJsonArray.length();i++){
							String authorId=authorsJsonArray.getJSONObject(i).getJSONObject("author").getString("key").replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);
						List<String> authorNames=authorIds.stream().map(id -> authorRepository.findById(id))
										  .map(optionalAuthor->{
											if(!optionalAuthor.isPresent()) {return "Unknown Author";}
											return optionalAuthor.get().getName();
										  }).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}
					bookRepository.save(book);
				} catch(JSONException e){
					e.printStackTrace();
				}
			});
		} catch(IOException e){
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start(){
		initAuthors();
		initWorks();
	}

}
