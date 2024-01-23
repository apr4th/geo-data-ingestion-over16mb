# geo-data-ingestion-over16mb

# Overview
In the realm of data management, handling high-density coordinates within a database can pose significant challenges, especially when dealing with size limitations imposed by data types. This article presents a solution that addresses the constraints associated with the Variant Data Type in Snowflake platform, specifically the 16MB size limitation.

The challenge emerged when attempting to load a set of high-density coordinates, formatted in JSON, into a table in Snowflake. The initial approach, utilizing a Variant column, encountered ingestion failures due to the inherent constraints of the data type. The article delves into the specifics of this limitation, shedding light on the obstacles faced by the customer during the data loading process.

# Solution
The proposed solution utilizes a parent-child table relationship. In the parent table, foundational geographical properties are stored for streamlined retrieval and management. For the intricate high-density coordinates, the dataset is carefully divided into multiple coordinate records, each stored in a dedicated child table.

This approach efficiently sidesteps the 16MB size constraint while introducing a scalable and organized structure for managing complex coordinate data. The child table serves as a repository for individual coordinate records, facilitating straightforward querying and retrieval based on specific criteria.

The article details the step-by-step implementation of this solution, offering a practical guide for storing expansive coordinate datasets. Through a comprehensive exploration of the reasoning behind this storage strategy, the article underscores the advantages of adopting a relational model for high-density coordinates.

By segmenting the dataset and distributing it across two interconnected tables, this solution strikes a balance between efficient data storage and simplified retrieval. The article concludes by emphasizing the impact of this strategic approach on overcoming data type limitations, providing a valuable resource for professionals navigating similar challenges in their data management endeavors.

# Prerequisites
No additional steps are necessary. The GitHub repository contains everything needed for this tutorial.

# Architecture for Ingesting High-Density Coordinates Data Sets
<img src="https://github.com/apr4th/geo-data-ingestion-over16mb/blob/main/images/shape-files-ingestion-1080x608.png?raw=true" alt="drawing" style="width:1080px;"/>

