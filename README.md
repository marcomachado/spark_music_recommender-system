# Music recommender system in PySpark using use the Alternating Least Squares (ALS) algorithm

### Audioscrobbler Dataset
That is, recommenders were usually viewed as tools that operated  on input like “Bob rates Prince 3.5 stars.” The Audioscrobbler dataset is interesting  because it merely records plays: “Bob played a Prince track.” A play carries less  information than a rating. Just because Bob played the track doesn’t mean he actually  liked it. You or I may occasionally play a song by an artist we don’t care for, or even  play an album and walk out of the room.  

However, listeners rate music far less frequently than they play music. A dataset  like this is therefore much larger, covers more users and artists, and contains more  total information than a rating dataset, even if each individual data point carries  less information. This type of data is often called implicit feedback data because the  user-artist connections are implied as a side effect of other actions, and not given as  explicit ratings or thumbs-up.

This data set contains profiles for around 150,000 real people 
The dataset lists the artists each person listens to, and a counter indicating how many times each user played each artist

The dataset is continually growing; at the time of writing (6 May 2005) Audioscrobbler is receiving around 2 million song submissions per day 

#### Files:
 - user_artist_data.txt: 3 columns: userid artistid playcount
 - artist_data.txt: 2 columns: artistid artist_name
 - artist_alias.txt: 2 columns: badid, goodid
    - known incorrectly spelt artists and the correct artist id. 
    - you can correct errors in user_artist_data as you read it in using this file (we're not yet finished merging this data)


#### Requirements
 - Implicit feedback: We need an algorithm that learns without access to user or artist  attributes
    - <b>collaborative filtering algorithms</b>: Deciding that two users might  both like the same song because they play many other songs that are the same is  an example.
- Sparsity
- Scalability and real-time predictions

#### Broad class of algorithms: latent factor models
 - try  to explain observed interactions between large numbers of users and items through  a relatively small number of unobserved, underlying reasons.
 - identify <b>latent factors</b> that could explain
    - <b>matrix factorization model</b>
        - these algorithms treat the user and product data as if it were a large matrix A
        - where the entry at row i and column j exists if user i has played artist j
        - The k columns correspond  to the latent factors that are being used to explain the interaction data
        - “matrix completion algorithms” -> original matrix A may be quite sparse, but the product XY<sup>T</sup> is dense


We will use the Alternating Least Squares algorithm to compute latent factors from  our dataset. This type of approach was popularized around the time of the Netflix  Prize competition by papers like “Collaborative Filtering for Implicit Feedback Datasets” and “Large-Scale Parallel Collaborative Filtering for the Netflix Prize”