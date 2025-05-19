# 🕵️ The Ghost in the Gears – SQL Investigation Case Study

This project is a multi-stage SQL investigation conducted using Snowflake to solve a fictional murder mystery called **"The Ghost in the Gears."** Over six assignments, I acted as a digital detective—cleaning, transforming, analyzing, and joining data to uncover suspects by combining crime records, forum activity, call logs, and video surveillance.

---

## 🧠 Problem Statement

A series of homicides involving AI activists has occurred across several districts. I was tasked with uncovering:

- **Who the victims were**
- **What patterns existed in their backgrounds and call history**
- **Whether city officials were involved**
- **If digital forums or surveillance footage contained clues**
- **Who the final suspects were based on intersecting behavioral data**

---

## 🔧 Tools & Technologies

- **Snowflake SQL**  
- **S3 External Staging**  
- **ETL with COPY INTO & FILE FORMATS**  
- **Views & CTEs**  
- **JSON Parsing (video activity)**  
- **Role-Based Access Control**  
- **Data Wrangling and Exploration**

---

## 📊 Data Sources

| Dataset                 | Description |
|-------------------------|-------------|
| `crime_details`         | Records of all crimes, including homicides |
| `victim_profiles`       | Personal info of crime victims |
| `forum_activities`      | Posts made by anonymous users about AI |
| `city_officials`        | Contact and IP data for officials |
| `phone_directory`       | Used to map calls to individuals |
| `call_log`              | Inbound and outbound call records |
| `video_activity_json`   | Surveillance logs from video cameras |

---

## 📂 Project Structure

- `full_investigation.sql`: Complete SQL script including setup, analysis, and final query
- `README.md`: Project overview and results

---

## 🧩 What I Did

### ✅ 1. **Environment Setup**
- Created warehouses and schema in Snowflake
- Implemented query timeouts and resource monitors
- Managed access control using custom roles (`ghosts_query_role`, `junior_detective`)

### ✅ 2. **Data Loading**
- Staged CSV/JSON files from S3 buckets using `COPY INTO`
- Defined file formats for structured and semi-structured data
- Loaded 7 datasets into Snowflake

### ✅ 3. **View Creation**
- Built views like:
  - `victim_detail_view`: joins victim, crime, and phone directory
  - `calls_to_victims`: filters calls ≤14 days before murder
  - `forum_posts_by_officials`: joins IPs to AI-related posts
  - `calls_to_victims_by_officials`: narrows to official suspects

### ✅ 4. **Data Exploration & Analysis**
- % of answered calls and avg call duration
- Crime trends by time, type, and victim profile
- Outbound call volume by district
- Cumulative monthly call trends
- Video activity on homicide dates

### ✅ 5. **Final Suspect Identification**
- Officials who both:
  - Made suspicious calls
  - Posted about AI
- Joined using `USING(name)` to narrow down true suspects

---

## 🔍 Final Outcome

> Based on call logs, forum data, and video activity:
> - A specific group of city officials emerged as suspects  
> - Their IPs matched AI forum posts  
> - They contacted victims days before each murder  
> - Several were seen near the crime scene in JSON footage  

This case highlights how structured SQL logic, combined with data enrichment, can be used for investigative intelligence.

---

## 🔐 Role Management

Created Snowflake roles and granted controlled access to views, warehouses, and schemas — simulating multi-user, secure investigation access.

---

## 📫 Let’s Connect

- 📧 arma.rahamath@gmail.com  
- 🔗 [LinkedIn](https://linkedin.com/in/armashaik)  
- 🧑‍💻 [GitHub](https://github.com/ArmaShaik)

---

