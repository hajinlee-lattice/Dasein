let timer;
function debounce(fn, time) {
  clearTimeout(timer);
  timer = setTimeout(() => {
    return fn.apply(this, arguments);
  }, time);
}

export default debounce;
