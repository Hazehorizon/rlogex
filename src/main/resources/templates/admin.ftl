<#include "header.ftl">

<nav class="navbar navbar-expand-lg navbar-light bg-light">
  <a class="navbar-brand" href="/admin">Logs Admin:</a>
  <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
    <span class="navbar-toggler-icon"></span>
  </button>

  <div class="collapse navbar-collapse" id="navbarSupportedContent">
      <ul class="navbar-nav mr-auto">
          <#if context.lastReadedDate??>
              <li class="nav-item active">
                <button type="button" class="btn btn-outline-success" data-toggle="modal" data-target="#changeDateModal">
                  ${context.lastReadedDate}
                </button>
              </li>
           </#if>
      </ul>

    <#if context.lastReadedDate??>
        <div class="modal fade" id="changeDateModal" tabindex="-1" role="dialog" aria-labelledby="changeDateModalLabel" aria-hidden="true">
          <div class="modal-dialog" role="document">
            <div class="modal-content">
              <div class="modal-header">
                <h5 class="modal-title" id="changeDateModalLabel">Last logs loaded date</h5>
                <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                  <span aria-hidden="true">&times;</span>
                </button>
              </div>
              <form class="form-inline" action="/admin/setDate" method="get" >
                  <div class="modal-body">
                     <input id="lastReadedDate" type="date" class="form-control mr-sm-2" name="lastReadedDate" value="${context.lastReadedDate}">
                  </div>
                  <div class="modal-footer">
                    <a href="/admin/resetDate" class="btn btn-secondary">Reset date</a>
                    <button class="btn btn-primary" type="submit">Save</button>
                  </div>
              </form>
            </div>
          </div>
        </div>
    </#if>
  </div>
</nav>

<div id="accordion">
  <#list context.files?keys as value>
      <div class="card">
        <div class="card-header" id="heading${value}">
          <h5 class="mb-0">
            <button class="btn btn-link collapsed" data-toggle="collapse" data-target="#collapse${value}" aria-expanded="false" aria-controls="collapse${value}">
              ${value}
            </button>
          </h5>
        </div>
    
        <div id="collapse${value}" class="collapse" aria-labelledby="heading${value}" data-parent="#accordion">
          <div class="card-body">
              <ul class="list-group list-group-flush">
                <#list context.files[value]?keys as group>
                    <li class="list-group-item">
                        <span class="badge badge-primary">${group}</span>
                        <#list context.files[value][group]?keys as node>
                            <span class="badge badge-info">${node}</span>
                            =
                            <a href="/admin/remove?key=${value + '_' + group + '_' + node}" class="btn btn-outline-danger">${context.files[value][group][node]?c}</a>
                        </#list>
                    </li>
                </#list>
              </ul>
              <div class="card-body">
                <a href="/admin/reload?date=${value}" class="btn btn-warning">Reload</a>
              </div>
          </div>
        </div>
      </div>
  </#list>
</div>

<#include "footer.ftl">