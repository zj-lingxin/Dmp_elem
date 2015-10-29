package com.asto.dmp.elem.service

import com.asto.dmp.elem.base.DataSource

trait Service extends DataSource {
  def run()
}

