package com.dedupeer.gui.component.renderer;

import java.awt.Component;

import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

import com.dedupeer.gui.component.model.StoredFileDataModel;
import com.dedupeer.navigation.DFolder;
import com.dedupeer.utils.FileUtils;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class IconLabelRenderer extends DefaultTableCellRenderer {

	private static final long serialVersionUID = 6335367333085684949L;
	
	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus, int row, int column) {
		
		ImageIcon icon = null;
		
		JLabel iconLabel = (JLabel) super.getTableCellRendererComponent(table, value, isSelected, hasFocus,
				row, column);
		
		if(((StoredFileDataModel) table.getModel()).getStoredFileByRow(row) instanceof DFolder) {
			if(column == 0) { //icon must appear only in the first column
				icon = (String) value != null ? FileUtils.getFolderIcon() : null;
			}				
		} else {
			icon = FileUtils.getIconByFileType((String) value);
		}		
		
		iconLabel.setIcon(icon);
		
		if(isSelected) {
			iconLabel.setBackground(table.getSelectionBackground());
			iconLabel.setForeground(table.getSelectionForeground());
		} else {
			iconLabel.setBackground(table.getBackground());
			iconLabel.setForeground(table.getForeground());
		}
		return iconLabel;
	}

}
