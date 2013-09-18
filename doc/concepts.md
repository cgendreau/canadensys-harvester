#Harvester concepts

##Job
A job is a collection of step and task. It is responsible to initiate the whole process.

##Step
Must implement `ProcessingStepIF`

A step should include a reader, a processor and a writer.

##Task
Must implement `ItemTaskIF`

A task does not include any reader, processor or writer. It could received some simple data using
the `sharedParameters`.

##Processor
Must implement `ItemProcessorIF<T,V>`

A processor take one object and process it into a new object.

##Mapper
Must implement `ItemMapperIF<T>`

A mapper map a set of properties values as string to a specific object.

##Reader
Must implement `ItemReaderIF<T>`

A reader reads from a data source and give an object back.

##Writer
Must implement `ItemWriterIF<T>`

A writer writes an object into a data source.