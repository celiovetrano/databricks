SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE SCHEMA airbnb;
GO

CREATE TABLE [airbnb].[listings](
	[id] [int] NULL,
	[name] [nvarchar](255) NULL,
	[host_id] [int] NULL,
	[host_name] [nvarchar](255) NULL,
	[neighbourhood_group] [nvarchar](50) NULL,
	[neighbourhood] [nvarchar](50) NULL,
	[latitude] [float] NULL,
	[longitude] [float] NULL,
	[room_type] [nvarchar](50) NULL,
	[price] [int] NULL,
	[minimum_nights] [int] NULL,
	[number_of_reviews] [int] NULL,
	[last_review] [date] NULL,
	[reviews_per_month] [float] NULL,
	[calculated_host_listings_count] [int] NULL,
	[availability_365] [int] NULL
) ON [PRIMARY]
GO
