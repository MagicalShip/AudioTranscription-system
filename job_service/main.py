from __future__ import annotations

@app.get("/health")
def health():
    return {"status": "ok", "service": "job-service"}


@app.post("/jobs", status_code=202)
async def create_job(
    request: Request,
    file: UploadFile = File(...),
    source_language: str = Form("en"),
    target_language: str = Form("zh"),
):
    if (source_language, target_language) != ("en", "zh"):
        raise HTTPException(400, "The assessed version supports English to Chinese only")
    filename = Path(file.filename or "audio").name
    extension = Path(filename).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, f"Supported formats: {sorted(ALLOWED_EXTENSIONS)}")
    active = await asyncio.to_thread(active_job_count)
    ACTIVE_JOBS.set(active)
    if active >= MAX_ACTIVE_JOBS:
        raise HTTPException(429, "The system is busy. Please try again later.")

    job_id = str(uuid4())
    object_name = f"source/{job_id}/{filename}"
    size = 0
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as temp:
            temp_path = temp.name
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(413, "Audio file exceeds the 200 MB limit")
                temp.write(chunk)
        await asyncio.to_thread(
            upload_file,
            request.app.state.storage,
            object_name,
            temp_path,
            file.content_type or "application/octet-stream",
        )
        await asyncio.to_thread(
            insert_job,
            {
                "id": job_id,
                "filename": filename,
                "source_language": source_language,
                "target_language": target_language,
                "object_name": object_name,
            },
        )
        JOBS_CREATED.inc()
        ACTIVE_JOBS.set(active + 1)
        return {"job_id": job_id, "status": "QUEUED", "message": "File accepted for processing"}
    finally:
        if temp_path:
            Path(temp_path).unlink(missing_ok=True)


@app.get("/jobs/{job_id}")
async def read_job(job_id: str):
    job = await asyncio.to_thread(get_job, job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.get("/jobs/{job_id}/segments")
async def read_segments(job_id: str):
    if not await asyncio.to_thread(get_job, job_id):
        raise HTTPException(404, "Job not found")
    return await asyncio.to_thread(get_segments, job_id)


@app.post("/jobs/{job_id}/retry-translation", status_code=202)
async def retry_translation(job_id: str):
    if not await asyncio.to_thread(queue_translation_retry, job_id):
        raise HTTPException(409, "No completed transcript is available")
    return {"job_id": job_id, "status": "TRANSLATING"}


@app.get("/", response_class=HTMLResponse)
def index():
    return """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Audio Transcription and Translation</title>
<style>
:root{color-scheme:light;font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#172033;background:#f4f7fb}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:linear-gradient(145deg,#eef5ff 0%,#f8f5ff 52%,#f7fafc 100%)}
.page{width:min(900px,calc(100% - 32px));margin:0 auto;padding:56px 0 72px}
h1{margin:0;font-size:clamp(30px,5vw,46px);line-height:1.1;letter-spacing:-.035em}
.card{margin-top:26px;background:rgba(255,255,255,.9);border:1px solid #e1e7f0;border-radius:18px;box-shadow:0 16px 45px rgba(36,50,83,.08);padding:24px}
.upload{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
input[type=file]{flex:1;min-width:250px;padding:14px;border:1px dashed #aeb9cc;border-radius:12px;background:#f8faff;color:#526072}
button{border:0;border-radius:11px;padding:14px 22px;background:#5266c9;color:white;font:inherit;font-weight:700;cursor:pointer;transition:.18s ease}
button:hover{background:#4356b2;transform:translateY(-1px)}
button:disabled{opacity:.55;cursor:wait;transform:none}
.hint{margin:14px 0 0;color:#788396;font-size:13px}
.progress{display:flex;align-items:center;gap:8px;margin:22px 0 0;padding-top:18px;border-top:1px solid #e8ecf3}
.step{flex:1;height:5px;border-radius:99px;background:#dfe4ed;transition:.25s ease}.step.active{background:#5266c9}
.status-row{display:flex;align-items:center;gap:10px;margin:18px 0 0;min-height:30px;color:#566175}
.status-badge{display:none;padding:6px 11px;border-radius:999px;background:#e9edff;color:#4052ae;font-size:13px;font-weight:750}
.status-badge.visible{display:inline-block}.status-badge.error{background:#ffebed;color:#ae3743}.status-badge.done{background:#e7f7ef;color:#18724a}
.results{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:22px}
.result-card{min-height:220px;background:rgba(255,255,255,.86);border:1px solid #e1e7f0;border-radius:16px;padding:22px;box-shadow:0 10px 32px rgba(36,50,83,.05)}
.result-card h2{margin:0 0 14px;font-size:17px}.language{float:right;color:#8791a2;font-size:12px;font-weight:600}
pre{margin:0;white-space:pre-wrap;word-break:break-word;font:15px/1.7 inherit;color:#354054}
.empty{color:#a1a9b6}
@media(max-width:680px){.page{padding-top:34px}.results{grid-template-columns:1fr}.result-card{min-height:170px}button{width:100%}}
</style>
</head>
<body><main class="page">
<h1>Audio Transcription & Translation</h1>

<section class="card">
  <form id="form" class="upload">
    <input id="file" name="file" type="file" accept="audio/wav,.wav" required>
    <input type="hidden" name="source_language" value="en">
    <input type="hidden" name="target_language" value="zh">
    <button id="submit" type="submit">Upload and Process</button>
  </form>
  <p class="hint">WAV only · Up to 30 minutes · Maximum 200 MB</p>
  <div class="progress" aria-label="Processing progress"><span class="step"></span><span class="step"></span><span class="step"></span></div>
  <div class="status-row"><span id="statusBadge" class="status-badge"></span><span id="statusText">Select an audio file</span></div>
</section>

<section class="results">
  <article class="result-card"><h2>English Transcript</h2><pre id="transcript" class="empty">Waiting</pre></article>
  <article class="result-card"><h2>Chinese Translation</h2><pre id="translation" class="empty">Waiting</pre></article>
</section>
</main>
<script>
const form=document.querySelector('#form'),button=document.querySelector('#submit');
const badge=document.querySelector('#statusBadge'),statusText=document.querySelector('#statusText');
const transcript=document.querySelector('#transcript'),translation=document.querySelector('#translation');
const steps=[...document.querySelectorAll('.step')];
const terminal=['DONE','PARTLY_DONE','FAILED'];
const labels={QUEUED:'Queued',TRANSCRIBING:'Transcribing',TRANSLATING:'Translating',DONE:'Done',PARTLY_DONE:'Partly done',FAILED:'Failed'};

function showStatus(state,error=''){
  const levels={QUEUED:1,TRANSCRIBING:1,TRANSLATING:2,DONE:3,PARTLY_DONE:3,FAILED:3};
  steps.forEach((step,index)=>step.classList.toggle('active',index<(levels[state]||0)));
  badge.textContent=labels[state]||state;badge.className='status-badge visible';
  if(state==='FAILED')badge.classList.add('error');
  if(state==='DONE')badge.classList.add('done');
  statusText.textContent=error||'Processing';
}
function showResult(element,text,placeholder){element.textContent=text||placeholder;element.classList.toggle('empty',!text)}

form.onsubmit=async(event)=>{
  event.preventDefault();button.disabled=true;button.textContent='Uploading…';
  statusText.textContent='Uploading';badge.className='status-badge';steps.forEach(s=>s.classList.remove('active'));
  try{
    const response=await fetch('/jobs',{method:'POST',body:new FormData(form)}),data=await response.json();
    if(!response.ok)throw new Error(data.detail||'Upload failed');
    showStatus(data.status);poll(data.job_id);
  }catch(error){badge.textContent='Error';badge.className='status-badge visible error';statusText.textContent=error.message;button.disabled=false;button.textContent='Upload and Process'}
};
async function poll(id){
  try{
    const response=await fetch('/jobs/'+id),data=await response.json();if(!response.ok)throw new Error('Unable to read status');
    showStatus(data.status,data.error||'');
    showResult(transcript,data.transcript,'Waiting for transcription');
    showResult(translation,data.translation,data.status==='TRANSLATING'?'Translating…':'Waiting for translation');
    if(!terminal.includes(data.status)){setTimeout(()=>poll(id),2000);return}
    button.disabled=false;button.textContent='Process Another File';
  }catch(error){statusText.textContent=error.message;setTimeout(()=>poll(id),3000)}
}
</script></body></html>
"""
