# GenAI Usage Documentation

Document your use of AI tools throughout this project.

---

## AI Tools Used

Cursor, GitHub Copliot

## How We Used AI

### Phase 1: Tekton Pipeline

**What AI helped with:**  
AI helped with planning the structure of the project, creating first drafts of the necessary config files, suggesting ideas for additional hardening. Ai was also helpful for boilerplate code and checking for bugs. Also, AI was very helpful with the use of the OpenShift cli.

**What AI got wrong:**

**Most failures were down to a lack of context given to the model. There weren't any major issues in Phase 1.**

### Phase 2: Spark/DB

What AI helped with:

Planning, writing the boilerplate code for schema definitions, helped a lot with the refactor towards a raw landing table in TimescaleDB. AI also helped with writing bash scripts.

**What AI got wrong:**  
Structural inefficiencies/defects were hard for AI to detect. AI initially suggested computing features straight from kafka topics. We pivoted, realizing it was more robust and more efficient to store the raw messages first into landing tables. Given that messages didn't arrive monotonically with time, it was important to be able to easily determine the time buckets that needed updating (which is easier if the messages were first stored in the database). Also bugs still occured, particularly when a certain file was updated but the AI wasn't aware of the the other files dependent on said file.

### Phase 3:

What AI helped with:

Boilerplate Streamlit code, helped figure out how to connect the streamlit app to the DB in OpenShift (through a port forward), helped with the ML model.

---

## Reflection

A lot of the concepts and frameworks used for this project were new to members of the team. AI removed all barriers of entry and made these concepts/frameworks instantly accessible. AI also allowed us to be more ambitious and thorough given the time constraints of the project.