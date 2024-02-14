package ru.itclover.tsp.http.utils

import org.zeroturnaround.zip.ZipUtil
import java.io.File

object ArchiveUtils {

  def packCSV(uuid: String) = {
    ZipUtil.pack(
      new File(s"/tmp/sparse_intermediate/$uuid"),
      new File(s"/tmp/sparse_intermediate/$uuid.zip")
    )
  }

}
