package spring.booktraker.dataloader.booktrakerdataloader.author;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AuthorRepository extends CassandraRepository<Author,String>{
    
}
