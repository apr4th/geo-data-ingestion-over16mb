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
<ol>
  <li>A Snowflake account is required to proceed with this tutorial.</li>
  <li>If you don’t have a sample Shape file, follow the steps in <a href="./shape-files/shape-files-location.pdf">shape-files-location.pdf</a> and download a sample Shape Zip File.</li>
</ol>

# Architecture for Ingesting High-Density Coordinates Data Sets
<img src="https://github.com/apr4th/geo-data-ingestion-over16mb/blob/main/images/shape-files-ingestion-1080x608.png?raw=true" alt="drawing" style="width:1080px;"/>

# Script Files Overview
The <a href="./scripts">script files</a> listed in the table below will be utilized in this tutorial.

<table border="1" style="border-collapse:collapse" width="100%">
  <tr style="color:#ffffff; background-color:#000000;">
    <td align="left" valign="bottom" width="30%"><b>FILE NAME</b></td>
    <td align="left" valign="bottom" width="70%"><b>DESCRIPTION</b></td>
  </tr>
  <tr>
    <td align="left" valign="top" width="30%">01-setup-environment.sql</td>
    <td align="left" valign="top" width="70%">This script file is used to set up the Snowflake environment and create necessary database objects.</td>
  </tr>
  <tr>
    <td align="left" valign="top" width="30%">02-stage-shape-files.sql</td>
    <td align="left" valign="top" width="70%">This script file is used for staging the sample shape zip file to the internal named stage GEO_STAGE.</td>
  </tr>
  <tr>
    <td align="left" valign="top" width="30%">03-create-procedure.sql</td>
    <td align="left" valign="top" width="70%">This script file is used for creating the procedure load_shapefile to ingest the geospatial data from the sample shape zip file.</td>
  </tr>
  <tr>
    <td align="left" valign="top" width="30%">04-cleanup-environment.sql</td>
    <td align="left" valign="top" width="70%">This script file is used for cleaning up the environment after this tutorial is completed.</td>
  </tr>
</table>

<br>
Mohanraj Palaniswamy - Sr. Solutions Architect
<br>
Please feel free to reach out me if you have any questions @ <a href = "mailto:mohanraj.palaniswamy@snowflake.com?subject=Feedback">mohanraj.palaniswamy@snowflake.com</a>
