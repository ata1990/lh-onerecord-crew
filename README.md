Our predection model is scheduled via Azure DataBricks Workflows. The workflows executes in sequence data ingestion from oneRecord and data preparation. Then the end-to-end ML flows executes in paralel. Last job is the PostCall towards oneRecordsAPI.
Try it out by following the flow as showed below. 
Note: you will need to set up an Azure Subcription, Azure DataBricks Workspace.
![image](https://github.com/ata1990/lh-onerecord-crew/assets/137679679/b7ac0cdc-6eeb-4488-87b7-76c75e553258)

For displaying the output, a demo app is created via AppSheet. Please check it on the link: 
https://www.appsheet.com/start/3757a841-4cca-43fe-8b36-07b423855c8f 
Short overview of it please refer to the video uploaded.
