-- Data:  data about almost 2,000 YouTube videos and comments

-- Question: What are the most commented-upon videos?
SELECT *
FROM videos
WHERE num_comments = (SELECT MAX(num_comments)
					 FROM videos_stats)

-- Question: What are the  most liked videos?
SELECT *
FROM videos
WHERE num_views = (SELECT MAX(num_views)
					 FROM videos)

-- Question: How many total views does each category have? How many likes?
SELECT keyword, SUM(num_views) AS total_views
FROM videos
GROUP BY keyword
ORDER BY total_views DESC

-- Question: What are the most-liked comments?
SELECT *
FROM comments
WHERE likes = (SELECT MAX(likes)
			  FROM comments)

-- Question: What is the ratio of views/likes per each category?
SELECT keyword, (SUM(num_likes) / SUM(num_views)) * 100 AS ratio_likes_to_views
FROM videos
GROUP BY keyword
ORDER BY ratio_likes_to_views DESC

-- Question: What is the average sentiment score in each keyword category?
SELECT keyword, AVG(sentiment) AS avg_sentiment
FROM videos v
LEFT JOIN comments c ON v.video_id = c.video_id
GROUP BY keyword
ORDER BY avg_sentiment DESC


-- Question: How many times do company names (i.e., Apple or Samsung) appear in each keyword category?
-- Apple
SELECT keyword, COUNT(video_id)
FROM videos
WHERE lower(title) LIKE '%samsung%'
OR lower(title) LIKE '%apple%'
GROUP BY keyword