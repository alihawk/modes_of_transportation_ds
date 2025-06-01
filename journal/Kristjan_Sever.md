# Kristjan Sever's Data Science Project Competition journal

## March 2025 (20h)
* 4. (1h): Getting to know each other, our strengths and weaknesses.
* 16. (5h): Playing with Folium maps and geopandas provided file.
* 17. (4h): Reading the association analysis paper applied to tourism provided by Tomaz.
* 18. (4h): Reading the affinity analysis paper applied to tourism provided by Tomaz.
* 20. (5h): Playing with data and reading documentation for libraries and data.

## April 2025 (15h)
* 5. (5h): Played with Medius Data a bit, made some initial distributions.
* 22. (5h): Read through OSRM API, which could prove useful for calculating how probable some point is based on distance from roads.
* 23. (5h): Start working on getting probabilities of each point, so we could use some threshold to filter outliers..

## May 2025 (103h)
* 2. (8h): Login to Arnes Cluster, set OTP, try to read from Juans home directory, and try to find a workaround since it was not possible. Try to set up a conda environment locally. Finally decide to just set the Singularity containers with desired conda environment. Try to get Jupyter notebooks to work by ssh to Arnes.
* 3. (8h): Play with Folium a bit, get a feel of how it works and what use-case we could have with it. Visualize a couple of transitions of users. Try to see any pattern, check how the users locations change, how much time passes, are the points useful, think about how to denoise it.
* 9. (4h): Get slovenian railway and road geojsons.
* 10. (7h): Visualize the geojsons a bit, think about how this could prove useful to the project (by getting more accurate distributions of modes of transportation across the specified bins).
* 11. (8h): Clean up the repo a bit. Work on transition matrix a bit. Fixing issue with .parquet files not opening as they should on my local machine. Talk with Juan about splitting the .parquets a bit to allow local work.
* 13. (3h): Meeting with Ali and Juan, discussing about cellphone towers, how to denoise it, what else we could do etc.
* 18. (5h): Fix issues with HPC Arnes not working for me, issues with Anaconda when running the nodes.
* 19. (6h): Create a workflow for opening Jupyter notebooks from Arnes HPC locally in my browser, learn about how it works and make it work.
* 20. (6h): Work on transition matrix logic, initial approach was to loop through all the rows and count the transitions. This was really slow so I searched for ways to optimize it
* 21. (6h): Speed up the process by processing it sequentially, this allowed me to play around with the data a bit, check which kind of rules we can implement and how much data would the rules remove from the whole set. I also played around with how to save the files, I had some issues with Arnes again, because I didn't have enough memory so i needed to solve that as well.
* 22. (6h): Think about and try it in the Jupyter notebook how to make a good rule for transitions. I had issues with users that had really sparse data, how to consider their movement. I didn't yet manage to solve it. Start writting report, a rough outline.
* 23. (5h): Watch videos and read material on Markov chains, on Monte Carlo method. Think about how it could be useful for our project.
* 24. (4h): Test some Markov Chains code online, get a feel for it.
* 25. (3h): Meeting with Ali and Juan, discuss about transition matrices, the rule for building it. Discuss what to do with Markov Chains. Think about how we could augment the pipeline to allow us to finetune parameters (tjough we dont have good validation set).
* 26. (6h): Work on finishing the project, check current work, writing report.
* 27. (5h): Before weekly meeting with Ali and Juan, cleaning github together. Discuss what to ask/do at the meeting. Update the report, clean up the images, think about which additional graphics we could use.
* 28. (5h): Updating the rule in transition matrix to count users based on how they move between bins. Send code to Juan, to check and finish. Discussions about modeling.
* 29. (3h): Running latest scripts, check data, try to get some interesting graphics for report, discussions about modeling.
* 30. (4h): Report editing, creating raw/cleaned map visualization, going through the report with the team.
* 31. (1h): Finalize Journal, cleanup github a bit.

## Weeklies: (7h)
* 25. 3.  (0.5h) Weekly
* 11. 3.  (1h) Deep dive  
* 18. 3.  (0.5h) Weekly
* 25. 3.  (0.5h) Weekly
* 1. 4.  (0.5h) Weekly
* 8. 4.  (0.5h) Weekly
* 15. 3.  (1h) Meeting with Tomaz
* 22. 4.  (0.5h) Weekly
* 6. 5.  (0.5h) Weekly
* 13. 5.  (0.5h) Weekly
* 20. 5.  (0.5h) Weekly
* 27. 5.  (0.5h) Final meeting

## Total: 145
