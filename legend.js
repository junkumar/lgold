var col = function(v) {
  if (v < 17) return "#008038";
  if (v < 20) return "#A3D396";
  if (v < 23) return "#FDD2AA";
  if (v < 26) return "#F7976B";
  if (v < 29) return "#F26123";
  if (v < 32) return "#E12816";
  return "#B7161E";
};

// Add the main panel
var vis = new pv.Panel()
    .width(w)
    .height(h)
    .top(30)
    .bottom(20)
    .anchor("center").add(pv.Label)
      .text("Hello, world!");


// Add the color bars for the color legend
vis.add(pv.Bar)
    .data(pv.range(14, 32.1, 3))
    .bottom(function(d) this.index * 12)
    .height(10)
    .width(10)
    .left(5)
    .fillStyle(function(d) col(14 + 3 * this.index))
    .lineWidth(null)
  .anchor("right").add(pv.Label)
    .textAlign("left")
    .text(function(d) d + " - " + (d + 3) + "%");

vis.render();
