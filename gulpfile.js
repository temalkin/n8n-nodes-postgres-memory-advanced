const path = require('path');
const fs = require('fs');
const { task, src, dest } = require('gulp');

task('build:icons', copyIcons);

function copyIcons() {
  const nodeSource = path.resolve('nodes', '**', '*.{png,svg}');
  const nodeDestination = path.resolve('dist', 'nodes');

  // Copy icons from nodes (MemoryPostgresAdvanced/postgresql.svg, WorkingMemoryTool/postgresql.svg)
  const nodeStream = src(nodeSource).pipe(dest(nodeDestination));

  // Copy credentials icons if directory exists (optional - won't fail if missing)
  const credPath = path.resolve('credentials');
  if (fs.existsSync(credPath)) {
    const credSource = path.resolve('credentials', '**', '*.{png,svg}');
    const credDestination = path.resolve('dist', 'credentials');
    src(credSource).pipe(dest(credDestination));
  }

  return nodeStream;
}
