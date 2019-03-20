<#include "header.ftl">

<nav class="navbar navbar-expand-lg navbar-light bg-light">
  <a class="navbar-brand" href="/logs">Logs:</a>
  <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
    <span class="navbar-toggler-icon"></span>
  </button>

  <div class="collapse navbar-collapse" id="navbarSupportedContent">
    <form class="form-inline w-100" action="/logs" method="get" >
      <div class="form-group row">
          <input class="form-control mr-sm-2" type="search" placeholder="Search" aria-label="Search" name="query" value="${context.query}">
          <button class="btn btn-outline-success mr-sm-2" type="submit">Search</button>
          <label for="startDateTime" class="mr-sm-2">Date interval:</label>
          <input id="startDateTime" type="datetime-local" class="form-control mr-sm-2" name="startDateTime" value="${context.startDateTime}">
          <label for="endDateTime" class="mr-sm-2">:</label>
          <input id="endDateTime" type="datetime-local" class="form-control mr-sm-2" name="endDateTime" value="${context.endDateTime}">
          <label for="count" class="mr-sm-2">Page size:</label>
          <input id="count" class="form-control mr-sm-2" type="number" min="100" step="100" max="10000" name="count" value="${context.count?c}"/>
      </div>
    </form>
    <#if context.page??>
      <ul class="navbar-nav">
        <li class="page-item">
           <span class="badge badge-secondary">Returned:${context.lines?size?c}</span>
        </li>
      <ul>
      <ul class="navbar-nav mr-auto pagination">
        <li class="page-item${context.previousPageUrl???then('',' disabled')}">
          <a class="page-link" href="${context.previousPageUrl!''}" aria-label="Previous">
            <span aria-hidden="true">&laquo;</span>
            <span class="sr-only">Previous</span>
          </a>
        </li>
        <li class="page-item"><a class="page-link">${(context.page + 1)?c}</a></li>
        <li class="page-item${(context.count>context.lines?size)?then(' disabled','')}">
          <a class="page-link" href="${context.nextPageUrl}" aria-label="Next">
            <span aria-hidden="true">&raquo;</span>
            <span class="sr-only">Next</span>
          </a>
        </li>
      </ul>
    </#if>    
  </div>
</nav>

<#if context.lines??>
    <div id="accordion">
       <#list context.lines as line>
          <div class="card">
            <div class="card-header p-0" id="heading${line.id}">
              <h5 class="mb-0">
                <button class="btn btn-link collapsed" data-toggle="collapse" data-target="#collapse${line.id}" aria-expanded="false" aria-controls="collapse${line.id}">
                  ${line.line?keep_before('\n')}
                </button>
              </h5>
            </div>
          </div>

          <div id="collapse${line.id}" class="collapse" aria-labelledby="heading${line.id}" data-parent="#accordion">
            <div class="card-body">
                <ul class="list-group list-group-flush">
                 <#list line?keys as name>
                    <#if name != 'line'>
                        <li class="list-group-item p-0">
                           <span class="badge badge-pill badge-light">${name}</span>:
                           <span class="badge badge-info">${line[name]}</span>
                        </li>
                    </#if>
                  </#list>
                  <li class="list-group-item p-0">
                    <span class="badge badge badge-light text-left" style="white-space: pre-wrap">${line['line']}</span>
                  </li>
                </ul>
            </div>
          </div>
       </#list>
    </div>
</#if>

<#include "footer.ftl">