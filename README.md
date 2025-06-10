### Setup
1. Clone the repository
2. Install dependencies
   ```pip install -r requirements.txt```

### Running the app
Type in the following command into the terminal window: 
```
python -m flask run
```
### Database Schema Analysis
The relational database schema is designed to manage biological study data involving subjects, their treatments, and the samples collected from them. It includes three main tables: Subjects, Treatments, and Samples. Each table serves a specific role in organizing the data. The Subjects table stores basic information about individuals, including their ID, medical condition, age, and sex. The Treatments table records interventions received by those subjects, along with whether or not the subject responded to the treatment. It links back to the Subjects table using a foreign key, allowing multiple treatments to be associated with each subject. Lastly, the Samples table captures data about biological samples taken from subjects. It records the project name, sample type, and immune cell counts (such as B cells and T cells), while also linking each sample to a subject through a foreign key.

The design is intentional and follows standard normalization practices. Separating data into these three related tables helps reduce redundancy, maintain data integrity, and simplify updates or extensions to the data structure in the future. For example, if a subject has multiple samples or treatments, these can be added without duplicating their basic information. Using foreign keys ensures that all treatments and samples are always connected to a valid subject, which is critical for maintaining the consistency of the dataset. Additionally, the inclusion of checks, such as ensuring immune cell counts are non-negative, helps enforce data quality.

As the volume of data increases, such as with hundreds of projects, thousands of samples, and more varied treatment conditions, this schema can scale well. Relational databases like MySQL with the InnoDB engine are optimized for handling large datasets, especially when indexes are used on frequently queried fields like subject_id, project, or treatment_name. The structure also supports complex queries and analytics, such as comparing immune profiles across treatment responses or analyzing trends across projects. It is flexible enough to support future expansions as well. For example, adding new immune markers, integrating sequencing data, or tracking sample collection locations could be done with additional linked tables.

Overall, the schema is clean, scalable, and well-suited for both transactional data entry and more advanced analytical workloads. It lays a strong foundation for storing biological research data and can grow with the increasing complexity and scale of future studies.
