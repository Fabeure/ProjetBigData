# Local Vibes – Music Trends by City  
**Big Data Architecture Project**  
*Streaming + Batch Processing + Real-Time Dashboarding*  

---

## 📌 Project Overview  
**Goal**: Compare musical tastes and listening habits across cities/countries by analyzing **Twitter (streaming) + Spotify (batch) data**.  

**Key Features**:  
- Real-time heatmap of trending genres per city.  
- Identification of local artists gaining traction.  
- "Genre battle" visualization (e.g., Pop vs. Rap vs. Rock).  

---

Our data pipeline consists of two main flows:

### Twitter Data Flow (Streaming)
1. Capture tweets with geolocation and music-related content
2. Process through Kafka streams
3. Analyze in real-time with Spark Streaming
4. Enrich with geographical and music metadata
5. Store in MongoDB

### Spotify Data Flow (Batch)
1. Daily extraction of top charts by country
2. Process and transform with Spark batch jobs
3. Join with Twitter data for enhanced insights
4. Generate aggregated metrics and trends
5. Update dashboard visualizations
---

### 2. **Target Architecture**  
```mermaid
flowchart TD
    %% Data Sources
    subgraph Data Sources
        Sp_chart[Spotify Daily Charts]
        SP_Batch[Spotify Batch: CSV]
        TW_Stream[TwitterStreaming] -->|Kafka| K1[Kafka Topic - Twitter]
 
    end

    %% Processing Layer
    subgraph Processing
        SP_Batch-->|Batch Load| SB2[Spark Batch - Spotify]
        K1 --> SS1[Spark Streaming - Twitter]
    end

    %% Unified NoSQL Storage
    subgraph Storage
        SS1 --> k2[Kafka topic- Result]
        SB2 --> NOSQL
        Sp_chart--> NOSQL
    end

    %% API + Dashboard
    subgraph Dashboard Layer
        NOSQL --> API[REST API]
        k2 --> API[REST API]
        API --> UI[Web Dashboard]
        UI --> HM[Heatmap]
        UI --> LA[Local Artists]
        UI --> GB[Genre Battle]
    end
```  
---

**Team**: Arij Thabet, Mohamed Saber Azzaouzi, Mohamed Hannachi, Skander Tebourbi (13)


