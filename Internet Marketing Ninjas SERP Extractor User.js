// ==UserScript==
// @name           Internet Marketing Ninjas SERP Extractor
// @namespace      Search
// @description    Extract Results from Search Results Pages
// @downloadURL https://www.internetmarketingninjas.com/seo-tools/get-urls-grease/imnExtract.user.js
// @updateURL https://www.internetmarketingninjas.com/seo-tools/get-urls-grease/imnExtract.user.js
// @version 18
// @include        https://www.google.com/search*
// @include        http://www.bing.com/*
// @include        *search.yahoo.com/*
// @include        *ask.com/web*
// @require        https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.15.2/xlsx.full.min.js
// @require        https://unpkg.com/file-saver@1.3.3/FileSaver.js
// ==/UserScript==


function getQueryVariable(variable) {
    var query = window.location.search.substring(1);
    var vars = query.split('&');
    for (var i = 0; i < vars.length; i++) {
        var pair = vars[i].split('=');
        if (decodeURIComponent(pair[0]) == variable) {
            return decodeURIComponent(pair[1]);
        }
    }
    console.log('Query variable %s not found', variable);
}

/* generate a download */
function s2ab(s) {
    var buf = new ArrayBuffer(s.length);
    var view = new Uint8Array(buf);
    for (var i=0; i!=s.length; ++i) view[i] = s.charCodeAt(i) & 0xFF;
    return buf;
}

function downloadUrlExcelFile(data) {
    // EXCEL
    /* this line is only needed if you are not adding a script tag reference */
    if(typeof XLSX == 'undefined') XLSX = require('xlsx');

    /* make the workbook */
    var wb = XLSX.utils.book_new();

    for (var j = 0; j < data.length; j++) {
        /* make the worksheet */
        let ws = XLSX.utils.json_to_sheet( data[j].values, {skipHeader: 1});
        XLSX.utils.book_append_sheet(wb, ws, data[j].name);
    }

    /* write workbook (use type 'binary') */
    var wbout = XLSX.write(wb, {bookType:'xlsx', type:'binary'});

    let searchTerm = getQueryVariable('q');
    saveAs(new Blob([s2ab(wbout)],{type:"application/octet-stream"}), searchTerm + "-google.xlsx");
}

function getListOfText(texts) {
    let values = [];
    let length = texts.snapshotLength;
    let j = 0;

    // all this nonsense is because xpath doesn't concat the text of sub elements.  We have to look for spaces and concat ourselves
    // <div>test <b>test<b> test</div>
    // Looking at the text content of the div would return 3 results, "test ", "test", and " test"
    // so we concat by hand looking for the spaces
    while (j < length) {
        let string = texts.snapshotItem(j).textContent;
        let lastChar = string.slice(-1);
        while (lastChar == ' ' || ((j + 1) < length && texts.snapshotItem(j + 1).textContent.charAt(0) == ' ')) {
            if(lastChar == ' ' ) {
                j++;
                string = string + texts.snapshotItem(j).textContent;
            } else if ((j + 1) < length && texts.snapshotItem(j + 1).textContent.charAt(0) == ' ') {
                j++;
                string = string + texts.snapshotItem(j).textContent;
            }
            lastChar = string.slice(-1);
        }
        values.push(string);
        j++;
    }
    return values;
}

function getListOfCleanHrefsFromAnchorElements(elements) {
    let hrefs = [];
    for (var j = 0; j < elements.snapshotLength; j++) {
        let href = elements.snapshotItem(j).getAttribute('href');
        if(href == null) {
            continue;
        }
        if (href.indexOf('//') === 0) {
            href = "https:" + href;
        }

        if (href.indexOf('.googleusercontent.com') != -1) {
            continue;
        }

        if (href.indexOf('.google.com') != -1 && href.indexOf('.googleadservices.com') == -1) {
            continue;
        }
        if (href.indexOf('http') != 0) {
            continue;
        }
        hrefs.push(href);
    }
    return hrefs;
}

var googleCategories = [
    {
        'xPath': "//div[@id='tads']//li[@class='ads-ad']//div[@class='ad_cclk']/a[not(@style)]",
        'name': "Top Ads",
        'listType': 'ul',
    },
    {
        'xPath': "//h3[text()='Videos']/../..//a",
        'name': "Videos",
        'listType': 'ul',
    },
    {
        'xPath': "//h2[text()='People also ask']/../..//div[contains(@class, 'match-mod-horizontal-padding hide-focus-ring')]/text()",
        'name': "People Also Ask",
        'listType': 'ul',
        'type': 'text',
    },
    {
        'xPath': "//div[@class='g mnr-c g-blk' and .//a[normalize-space(text())='About Featured Snippets']]//div[@class='r']/a",
        'name': "Snippet",
        'listType': 'ul',
    },
    {
        'xPath': "//div/h1[text()='Complementary Results']/..//a",
        'name': "Right Side Box",
        'listType': 'ul',
    },
    {
        'xPath': "//h2[text()='Local Results']/..//div[text()='Website']/../..",
        'name': "Map",
        'listType': 'ul',
    },
    {
        'xPath': "//div[@id='search']//div[@class='rc']//a",
        'name': "Organic Results",
        'listType': 'ol',
    },
    {
        'xPath': "//div[@id='bottomads']//li[@class='ads-ad']//div[@class='ad_cclk']/a[not(contains(@style,'display:none'))]",
        'name': "Bottom Ads",
        'listType': 'ul',
    },
    {
        'xPath': "//div[text()='Related search']/../div//a//div[contains(@class, 'ellip')]//text()",
        'name': "Related Search",
        'listType': 'ul',
        'type': 'text',
    },
    {
        'xPath': "//div[@id='bres']//div[@class='VLkRKc']//text()",
        'name': "Other Topics",
        'listType': 'ul',
        'type': 'text',
        'subValueXPath': "//div[@class='VLkRKc' and text()='{{text}}']/../../..//a[@title]/@title"
    },

    {
        'xPath': "//div[@class='brs_col']//a//text()",
        'name': "Searches Related To",
        'listType': 'ul',
        'type': 'text',
    },


];


if ( location.href.indexOf('google.com/search') != -1 && location.href.indexOf('tbm=') == -1 ) {
    let exportData = [];

    for (let a = 0; a < googleCategories.length; a++) {
        let tags = document.evaluate(googleCategories[a].xPath, document, null,XPathResult.UNORDERED_NODE_SNAPSHOT_TYPE, null);
        if (tags.snapshotLength == 0) {
            continue;
        }

        let values = [];
        if(googleCategories[a].type == 'text') {
            values = getListOfText(tags);
        } else {
            values = getListOfCleanHrefsFromAnchorElements(tags);
        }
        if (googleCategories[a].subValueXPath !== undefined) {
            for (var j = 0; j < tags.snapshotLength; j++) {
                let topic = tags.snapshotItem(j).textContent;
                let xPath = googleCategories[a].subValueXPath.replace("{{text}}", topic);
                let subTags = document.evaluate(xPath, document, null,XPathResult.UNORDERED_NODE_SNAPSHOT_TYPE, null);
                values = getListOfText(subTags);
                let data = {
                    name: "Topic - " + topic,
                    listType: googleCategories[a].listType,
                    values: [],
                };
                for (let i = 0; i < values.length; i++) {
                    data.values.push({value: values[i]});
                }
                exportData.push(data);
            }
        } else {
            let data = {
                name: googleCategories[a].name,
                listType: googleCategories[a].listType,
                values: [],
            };
            for (let i = 0; i < values.length; i++) {
                data.values.push({value: values[i]});
            }
            exportData.push(data);
        }




    }

    let div = '<div width="100%">';
    for (let x = 0; x < exportData.length; x++) {
        div += '<h2>' + exportData[x].name + '</h2>';
        div += '<' + exportData[x].listType + ' id="anaydenov">';
        for (let i = 0; i < exportData[x].values.length; i++) {
            let style = '';
            if (exportData[x].listType == 'ol') {
                style = 'style="list-style:decimal"';
            }
            div += '<li ' + style + '>';
            if(exportData[x].values[i].value.startsWith("http")) {
                div += "<a href='" + exportData[x].values[i].value + "'>" + exportData[x].values[i].value + "</a>";
            } else {
                div += exportData[x].values[i].value;
            }
            div += '</li>';
        }
        div += '</' + exportData[x].listType + '>';
    }

    div += '<hr/><br /><br /></div>';
    div += '<button id="downloadUrlExcelFile">Download Excel</button><br />';

    document.getElementById('center_col').innerHTML += div;

    document.getElementById('downloadUrlExcelFile').addEventListener('click', function(){
        downloadUrlExcelFile(exportData)
    });



}

if ( location.href.indexOf('yahoo.com/search') != -1 ) {
    var listItems = document.getElementsByClassName('ac-algo');
    var href = "";
    var subPattern=new RegExp(/RU=(http.*?)\/RK/ig);
    div = '<div width="100%">';
    for ( var i=0, len=listItems.length; i<len; ++i ){
        var yahooUrl = listItems[i].href;
        var results = subPattern.exec(yahooUrl);
        if(!results[1]){
            continue;
        }
        var url = decodeURIComponent(results[1]);
        //filter out the "sub" results that appear beneath a site
        // The old expression, replace with pattern search above: if (url.search("rds.yahoo.com") == -1 )
        if (!subPattern.test(url)){
            href = url + "<br />";
            div += href;
        }
    }
    div += '</div>';
    document.getElementById('web').innerHTML += div;
}

if ( location.href.indexOf('ask.com/web') != -1 ) {
    var listItems = document.getElementsByClassName('result-link');
    var href = "";
    div = '<div width="100%">';
    for ( var i=0, len=listItems.length; i<len; ++i ){
        var url = listItems[i].href;
        href = url + "<br />";
        div += href;
    }
    div += '</div>';
    document.getElementsByClassName('l-mid-content')[0].innerHTML += div;
}

if ( location.href.indexOf('bing.com/') != -1 ) {
    var Bresults = document.getElementById('results');
    var BlistItems = Bresults.getElementsByTagName('li');
    var text = "";
    for ( var i=0, len=BlistItems.length; i<len; ++i ){
        var headers = BlistItems[i].getElementsByTagName('h3');
        if (headers.length != 0) {
            text += headers[0].getElementsByTagName('a')[0];
            text += "<br />";
        }
    }
    document.getElementById('results').innerHTML += text;
}


