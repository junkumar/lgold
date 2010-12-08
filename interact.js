// Sorry Javascript, the browser is not the computer !
// A dumper method for debugging - use alongside inspector
function writeObj(obj, message) {
    if (!message) {
        message = obj;
    }
    var details = "*****************" + "n" + message + "n";
    var fieldContents;
    for (var field in obj) {
        fieldContents = obj[field];
        if (typeof(fieldContents) == "function") {
            fieldContents = "(function)";
        }
        details += "  " + field + ": " + fieldContents + "n";
    }
    console.log(details);
}


// Make Polys in the map
function createPolygon(riverSeg, color, weight, opacity, fillColor, fillOpacity, opts, map, label) {
    //        GPolygon(<GLatLng array> latlngs, <String> color,
    //        <Number> weight, <Number> opacity, <String> fillColor,
    //        <Number> fillOpacity, opts);

    //    var poly = new GPolygon(riverSeg, "#000000", 1, 1, "red", 0.5, {clickable : false});
    var poly = new GPolygon(riverSeg, color, weight, opacity, fillColor, fillOpacity, opts);


    var polys = [poly];

    // Listener for polygon clicks
    GEvent.addListener(map, "click",
                       function(overlay, point) {
                           if (!overlay) {
                               //        writeObj(point);
                               for (var i = 0; i < polys.length; i++) {
                                   if (polys[i].Contains(point)) {
                                       map.openInfoWindowHtml(point, label);
                                   }
                               }
                           }
                       });

/*
    // Listener for polygon mouseover
    GEvent.addListener(poly, "mouseover",
		       function(point) {
			   if (!defined point) point = map.getCenter();
			   writeObj(point);
			   writeObj(point);
			   for (var i = 0; i < polys.length; i++) {
			       if (polys[i].Contains(point)) {
				   map.openInfoWindowHtml(point, "<b>You clicked on</b> " + labels[i]);
			       }
			   }
		       });

    // Listener for polygon mouseout
    GEvent.addListener(poly, "mouseout",
		       function(point) {
			   if (!defined point) point = map.getCenter();
			   writeObj(point);
			   writeObj(point);
			   for (var i = 0; i < polys.length; i++) {
			       if (polys[i].Contains(point)) {
				   map.openInfoWindowHtml(point, "<b>You clicked on</b> " + labels[i]);
			       }
			   }
		       });
*/
    return poly;
}

//  This function displays the tooltip
// it can be called from an icon mousover or a side_bar mouseover
function showTooltip(marker, map) {
    var tooltip = document.getElementById("tooltip");
    tooltip.innerHTML = marker.tooltip;
    var point =
        map.getCurrentMapType().getProjection().fromLatLngToPixel(map.getBounds().getSouthWest(),
                                                                  map.getZoom());

    var offset =
        map.getCurrentMapType().getProjection().fromLatLngToPixel(marker.getPoint(),
                                                                  map.getZoom());

    var anchor = marker.getIcon().iconAnchor;
    var width = marker.getIcon().iconSize.width;
    var pos = new GControlPosition(G_ANCHOR_BOTTOM_LEFT,
                                   new GSize(offset.x - point.x -
                                             anchor.x + width,
                                             -offset.y + point.y +
                                             anchor.y));
    pos.apply(tooltip);
    tooltip.style.visibility = "visible";
}


// A function to create the marker and set up the event window
// Dont try to unroll this function. It has to be here for the function closure
// Each instance of the function preserves the contends of a different instance
// of the "marker" and "html" variables which will be needed later when the event triggers.
function createMarker(point, label, map, icon) {
    if (!icon) {
        icon = new GIcon(G_DEFAULT_ICON);
        icon.image = "darkgray-circle-icon-10.png";
        // if (map.getCurrentMapType().getName() === "Chalk") {
        //    Icon.image = "gray-circle-icon-12.png";
        // }
        icon.iconSize = new GSize(9, 9);
        icon.shadowSize = new GSize(0, 0);
        icon.iconAnchor = new GPoint(3, 3);
    }
    var marker = new GMarker(point, {
                                 icon: icon
                             });
    
    GEvent.addListener(marker, "click",
                       function() {
                           // marker.openInfoWindowHtml(label);
                           showTooltip(marker, map);
                       });

    marker.tooltip = label;
    //    The new marker "mouseover" and "mouseout" listeners
    GEvent.addListener(marker, "mouseover",
		       function() {
			   showTooltip(marker, map);
		       });

    GEvent.addListener(marker, "mouseout",
                       function() {
                         document.getElementById("tooltip").style.visibility = "hidden";
                       });
    return marker;
}

// A method for testing if a point is inside a polygon
// Returns true if poly contains point
// Algorithm shamelessly stolen from http://alienryderflex.com/polygon/
GPolygon.prototype.Contains = function(point) {
    var j = 0;
    var oddNodes = false;
    var x = point.lng();
    var y = point.lat();
    for (var i = 0; i < this.getVertexCount(); i++) {
        j++;
        if (j == this.getVertexCount()) {
            j = 0;
        }
        if (((this.getVertex(i).lat() < y) && (this.getVertex(j).lat() >= y))
            || ((this.getVertex(j).lat() < y) && (this.getVertex(i).lat() >= y))) {
            if (this.getVertex(i).lng() + (y - this.getVertex(i).lat())
                / (this.getVertex(j).lat() - this.getVertex(i).lat())
                * (this.getVertex(j).lng() - this.getVertex(i).lng()) < x) {
                oddNodes = !oddNodes;
            }
        }
    }
    return oddNodes;
};


// Main function that is a non-automated test
function runTestOverlays() {
    if (GBrowserIsCompatible()) {
        // Display the map, with some controls and set the initial location
        var map = new GMap2(document.getElementById("map"));
        map.addControl(new GLargeMapControl());
        map.addControl(new GMapTypeControl());
        map.setCenter(new GLatLng(32.8122666, -114.5149494), 8);


        // Set up marker mouseover tooltip div
        var tooltip = document.createElement("div");
        document.getElementById("map").appendChild(tooltip);
        //                        tooltip.style.backgroundColor="#e0e0e0";
        tooltip.id = "tooltip";
        tooltip.style.visibility = "hidden";


        // Set up poly
        var riverSeg = [new GLatLng(32.8122666, -114.88103548762578),
                        new GLatLng(32.8122666, -114.14886331237423),
                        new GLatLng(33.71946626, -114.06213214968372),
                        new GLatLng(33.71946626, -114.9338888503163)];

        var poly = createPolygon(riverSeg, "#000000", 1, 1, "red", 0.5, {
            clickable: false
        }, map);
        // map.addOverlay(poly);


        // Set up three markers with info windows
        var point1 = new GLatLng(34.29557035, -114.140225);
        var marker = createMarker(point1, "YYO 1", map);
        map.addOverlay(marker);

        var point2 = new GLatLng(33.71946626, -114.4980105);
        var marker2 = createMarker(point2, "2", map);
        map.addOverlay(marker2);

        var point3 = new GLatLng(32.8122666, -114.5149494);
        var marker3 = createMarker(point3, "3", map);
        map.addOverlay(marker3);

    }

    // display a warning if the browser was not compatible
    else {
        alert("Sorry, the Google Maps API is not compatible with this browser");
    }
}
